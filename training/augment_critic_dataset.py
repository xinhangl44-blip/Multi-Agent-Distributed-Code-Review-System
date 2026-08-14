import json
import random
from openai import OpenAI
from kafka import KafkaConsumer

client = OpenAI(
    api_key="ollama",
    base_url="http://127.0.0.1:11434/v1"
)
KAFKA_BOOTSTRAP = "127.0.0.1:9092"
LOCAL_MODEL_NAME = "qwen2.5:7b"

def get_real_llm_label(bug, patch):
    prompt = (
        f"Bug Report: {bug}\n"
        f"Proposed Patch:\n{patch}\n\n"
        "Analyze strictly. Output a clean JSON with keys:\n"
        "'thought': Reason within 50 words in English regarding safety and correctness.\n"
        "'decision': Exactly 'PASS' or 'FAIL'."
    )
    response = client.chat.completions.create(
        model=LOCAL_MODEL_NAME,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a senior code review expert. Respond ONLY in standard JSON format."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1
    )
    return json.loads(response.choices[0].message.content.strip())

def corrupt_patch(patch: str) -> str:
    corruptions = [
        lambda p: p.replace("mu.Lock()", "// mu.Lock() removed"),
        lambda p: p.replace("defer mu.Unlock()", "go func() {}()"),
        lambda p: p + " // syntax_error_tail_}",
        lambda p: "if true { panic(\"broken\") }"
    ]
    return random.choice(corruptions)(patch)

if __name__ == "__main__":
    try:
        consumer = KafkaConsumer(
            "agent.coder.results",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=3000
        )
        historical_patches = []
        for msg in consumer:
            payload = msg.value["payload"]
            historical_patches.append({
                "bug": payload["data"]["bug_report"],
                "patch": payload["data"]["patch"]["content"]
            })
        consumer.close()
    except Exception:
        exit(1)
    augmented_samples = []
    target_count = 200
    iteration = 0
    while len(augmented_samples) < target_count:
        for base in historical_patches:
            if len(augmented_samples) >= target_count:
                break
            is_negative = (iteration % 2 == 1)
            current_bug = base["bug"] + f" [Variant Cluster #{iteration}]"
            current_patch = corrupt_patch(base["patch"]) if is_negative else base["patch"]
            label_json = get_real_llm_label(current_bug, current_patch)
            sft_format = {
                "messages": [
                    {"role": "system", "content": "You are a lightweight Critic Agent. Evaluate the patch and return JSON."},
                    {"role": "user", "content": f"Review this patch:\n{current_patch}\nFor bug: {current_bug}"},
                    {"role": "assistant", "content": json.dumps(label_json, ensure_ascii=False)}
                ]
            }
            augmented_samples.append(sft_format)
        iteration += 1
    with open("critic_train_data.jsonl", "w", encoding="utf-8") as f:
        for sample in augmented_samples:
            f.write(json.dumps(sample, ensure_ascii=False) + "\n")
