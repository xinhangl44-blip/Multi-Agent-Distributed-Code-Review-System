import json
import random
from openai import OpenAI
from kafka import KafkaConsumer

# 🌟 2026 本地平替：连接本地常驻的 Ollama 服务
client = OpenAI(
    api_key="ollama",                     # Ollama 不需要密钥，随便填一个纯英文字符串占位
    base_url="http://127.0.0.1:11434/v1"  # Ollama 本地标准 API 端点
)

KAFKA_BOOTSTRAP = "127.0.0.1:9092"
# 🌟 【核心配置】请将这里改为你刚刚通过 `ollama list` 查看到的本地模型名称
LOCAL_MODEL_NAME = "qwen2.5:7b" 

def get_real_llm_label(bug, patch):
    """呼叫本地 Ollama 专家模型进行无噪声代码审查标注"""
    prompt = (
        f"Bug Report: {bug}\n"
        f"Proposed Patch:\n{patch}\n\n"
        "Analyze strictly. Output a clean JSON with keys:\n"
        "'thought': Reason within 50 words in English regarding safety and correctness.\n"
        "'decision': Exactly 'PASS' or 'FAIL'."
    )
    
    # 🌟 Ollama 原生支持通过 response_format 强制约束输出为标准 JSON
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
    """物理变异：故意将正常补丁魔改成 FAIL 补丁，制造高质量负样本"""
    corruptions = [
        lambda p: p.replace("mu.Lock()", "// mu.Lock() removed"),
        lambda p: p.replace("defer mu.Unlock()", "go func() {}()"), 
        lambda p: p + " // syntax_error_tail_}",
        lambda p: "if true { panic(\"broken\") }"
    ]
    return random.choice(corruptions)(patch)

if __name__ == "__main__":
    print("📥 开始从 Week 6 骨干网回收历史压测 Patch...")
    
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
    except Exception as e:
        print(f"❌ Kafka 读取失败: {str(e)}。请确保 Kafka 容器或服务已拉起！")
        exit(1)

    print(f"📋 成功抓取到 {len(historical_patches)} 条真实基础语料。")
    
    augmented_samples = []
    target_count = 200
    print(f"🧬 开始通过本地 [{LOCAL_MODEL_NAME}] 衍生并标注数据，目标：{target_count} 条...")

    iteration = 0
    while len(augmented_samples) < target_count:
        for base in historical_patches:
            if len(augmented_samples) >= target_count:
                break

            is_negative = (iteration % 2 == 1)
            current_bug = base["bug"] + f" [Variant Cluster #{iteration}]"
            current_patch = corrupt_patch(base["patch"]) if is_negative else base["patch"]

            # 🌟 呼叫本地大模型。如果报错（例如 Ollama 没开或模型不存在），会直接抛出，方便排查
            label_json = get_real_llm_label(current_bug, current_patch)

            sft_format = {
                "messages": [
                    {"role": "system", "content": "You are a lightweight Critic Agent. Evaluate the patch and return JSON."},
                    {"role": "user", "content": f"Review this patch:\n{current_patch}\nFor bug: {current_bug}"},
                    {"role": "assistant", "content": json.dumps(label_json, ensure_ascii=False)}
                ]
            }
            augmented_samples.append(sft_format)

            if len(augmented_samples) % 20 == 0:
                print(f"⏳ 进度: {len(augmented_samples)}/{target_count} 条已完成标注...")
        iteration += 1

    with open("critic_train_data.jsonl", "w", encoding="utf-8") as f:
        for sample in augmented_samples:
            f.write(json.dumps(sample, ensure_ascii=False) + "\n")

    print(f"🎉 200 条纯本地生成的黄金弹药库构建完毕！请立刻重启微调！")
