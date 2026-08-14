import json
import time
import requests
from openai import OpenAI

# ==========================================
# 1. 拓扑配置（复用你本地健全的 8000 和 8001 接口）
# ==========================================
VLLM_ENDPOINT = "http://localhost:8000/v1"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"
RAG_SERVICE_URL = "http://127.0.0.1:8001/retrieve"

vllm_client = OpenAI(base_url=VLLM_ENDPOINT, api_key="vllm-shared-token")

# ==========================================
# 2. 构造多元化的原始语料基础
# ==========================================
# 建议至少准备 30-50 个不同的场景，通过循环或改动派生出 100+ 条数据
raw_bug_scenarios = [
    {"bug": "KubeGPU Scheduler deadlocks when binding pods concurrently.", "is_good_patch": True},
    {"bug": "KubeGPU Scheduler deadlocks when binding pods concurrently.", "is_good_patch": False}, # 故意混入坏 Patch
    {"bug": "Memory leak in pod affinity cache cleaner loop.", "is_good_patch": True},
    {"bug": "Memory leak in pod affinity cache cleaner loop.", "is_good_patch": False},
    {"bug": "GPU fragmentation increases due to non-contiguous allocation.", "is_good_patch": True}
]

def generate_mock_patch(bug, is_good):
    if is_good:
        return "mu.Lock(); defer mu.Unlock(); if !allocated { bindPod() }"
    else:
        return "if !allocated { go bindPod() }" # 致命的不加锁并发，妥妥的 FAIL

# ==========================================
# 3. 数据蒸馏核心逻辑
# ==========================================
print("🚀 [Distill] 开始大模型数据蒸馏，正在准备训练粮食...")

distilled_samples = []

for idx, scenario in enumerate(raw_bug_scenarios):
    # a. 调度 RAG 抓取真实代码上下文
    try:
        resp = requests.post(RAG_SERVICE_URL, json={"query": scenario["bug"], "limit": 1}, timeout=5)
        hits = resp.json().get("data", [])
        context = hits[0].get('code_snippet', 'func Schedule() { // mesh }')[:400] if hits else "No context"
    except Exception:
        context = "func Schedule() { // KubeGPU scheduler mesh telemetry loop\n  mu.Lock()\n  // rigid logic\n}"

    patch = generate_mock_patch(scenario["bug"], scenario["is_good_patch"])

    # b. 构造强引导 Prompt，逼迫 7B 大模型吐出高质量的判别思维链（CoT）
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

    # c. 物理呼叫本地 vLLM
    start_time = time.time()
    response = vllm_client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        temperature=0.2, # 低随机度保证逻辑稳定
        response_format={"type": "json_object"} # 锁定 JSON 输出
    )
    
    raw_output = response.choices[0].message.content
    print(f"📥 [Sample {idx+1}/{len(raw_bug_scenarios)}] 蒸馏耗时: {time.time()-start_time:.2f}s")

    try:
        # 解析并转化为适合 SFT/QLoRA 训练的 Alpaca 或 ShareGPT 标准格式
        parsed_json = json.loads(raw_output)
        
        # 组装用于训练小模型的标准消息格式
        sft_format = {
            "messages": [
                {"role": "system", "content": "You are a lightweight Critic Agent. Evaluate the patch and return JSON."},
                {"role": "user", "content": f"Review this patch:\n{patch}\nFor bug: {scenario['bug']}"},
                {"role": "assistant", "content": json.dumps(parsed_json, ensure_ascii=False)}
            ]
        }
        distilled_samples.append(sft_format)
    except Exception as e:
        print(f"⚠️ 解析失败，跳过该样本: {str(e)}")

# ==========================================
# 4. 持久化落盘为 jsonl
# ==========================================
output_file = "critic_train_data.jsonl"
with open(output_file, "w", encoding="utf-8") as f:
    for sample in distilled_samples:
        f.write(json.dumps(sample, ensure_ascii=False) + "\n")

print(f"🎉 [Done] 成功导出 {len(distilled_samples)} 条高质量蒸馏数据至 {output_file}！")
