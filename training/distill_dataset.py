import json
import requests
from openai import OpenAI

VLLM_ENDPOINT = "http://localhost:8000/v1"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"
RAG_SERVICE_URL = "http://127.0.0.1:8001/retrieve"
vllm_client = OpenAI(base_url=VLLM_ENDPOINT, api_key="vllm-shared-token")
raw_bug_scenarios = [
    {"bug": "KubeGPU Scheduler deadlocks when binding pods concurrently.", "is_good_patch": True},
    {"bug": "KubeGPU Scheduler deadlocks when binding pods concurrently.", "is_good_patch": False},
    {"bug": "Memory leak in pod affinity cache cleaner loop.", "is_good_patch": True},
    {"bug": "Memory leak in pod affinity cache cleaner loop.", "is_good_patch": False},
    {"bug": "GPU fragmentation increases due to non-contiguous allocation.", "is_good_patch": True}
]

def generate_mock_patch(bug, is_good):
    if is_good:
        return "mu.Lock(); defer mu.Unlock(); if !allocated { bindPod() }"
    else:
        return "if !allocated { go bindPod() }"
        
distilled_samples = []
for scenario in raw_bug_scenarios:
    try:
        resp = requests.post(RAG_SERVICE_URL, json={"query": scenario["bug"], "limit": 1}, timeout=5)
        hits = resp.json().get("data", [])
        context = hits[0].get('code_snippet', 'func Schedule() { }')[:400] if hits else "No context"
    except Exception:
        context = "func Schedule() {\n  mu.Lock()\n}"
    patch = generate_mock_patch(scenario["bug"], scenario["is_good_patch"])
    system_prompt = (
        "You are an elite K8s senior controller reviewer. Analyze the patch strictly.\n"
        "You must output in a fixed JSON format with two keys:\n"
        "1. 'thought': Explain why this patch works or fails within 80 words.\n"
        "2. 'decision': Must be exactly 'PASS' or 'FAIL'."
    )
    user_content = (
        f"Bug Report: {scenario['bug']}\n"
        f"Code Context:\n{context}\n"
        f"Proposed Patch:\n{patch}"
    )
    try:
        response = vllm_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )
        raw_output = response.choices[0].message.content
        parsed_json = json.loads(raw_output)
        sft_format = {
            "messages": [
                {"role": "system", "content": "You are a lightweight Critic Agent. Evaluate the patch and return JSON."},
                {"role": "user", "content": f"Review this patch:\n{patch}\nFor bug: {scenario['bug']}"},
                {"role": "assistant", "content": json.dumps(parsed_json, ensure_ascii=False)}
            ]
        }
        distilled_samples.append(sft_format)
    except Exception:
        pass
output_file = "critic_train_data.jsonl"
with open(output_file, "w", encoding="utf-8") as f:
    for sample in distilled_samples:
        f.write(json.dumps(sample, ensure_ascii=False) + "\n")
