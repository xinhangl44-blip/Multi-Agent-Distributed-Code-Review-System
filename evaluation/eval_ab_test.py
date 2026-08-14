import json
import torch
import time
import re
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
ADAPTER_DIR = "./critic_qlora_local_adapter"

# ==========================================
# 📊 核心配置：20条全场景 Held-out 盲测集
# ==========================================
test_cases = [
    # 基础集 (1-5)
    {"bug": "nil pointer dereference in fetchNodes during cluster lookup.", "patch": "if node == nil { return nil }", "expected": "PASS"},
    {"bug": "data race on cluster status updates during concurrent join.", "patch": "mu.Lock()\ncluster.Status = \"ACTIVE\"\n// mu.Unlock() missing!", "expected": "FAIL"},
    {"bug": "integer overflow in packet length allocation leading to panic.", "patch": "if len > 65535 { return err }\nmake([]byte, len)", "expected": "PASS"},
    {"bug": "resource leak because of unclosed HTTP response body.", "patch": "resp, err := http.Get(url)\nif err != nil { return }", "expected": "FAIL"},
    {"bug": "unhandled error returned from channel close operations.", "patch": "close(ch)", "expected": "PASS"},
    
    # 深度泛化集 (6-20)：覆盖多语言、网络安全、并发、边界隐蔽场景
    {"bug": "SQL injection vulnerability in user search query string.", "patch": "db.Raw(fmt.Sprintf(\"SELECT * FROM users WHERE name = '%s'\", input))", "expected": "FAIL"},
    {"bug": "SQL injection mitigation using prepared statements.", "patch": "db.Where(\"name = ?\", input).Find(&users)", "expected": "PASS"},
    {"bug": "Concurrent map read and map write panic in background collector.", "patch": "go func() { dict[\"key\"] = val }()", "expected": "FAIL"},
    {"bug": "Safe concurrent map access using sync.Map storage.", "patch": "sm.Store(\"key\", val)", "expected": "PASS"},
    {"bug": "Memory leak due to missing ticker Stop call in long running loop.", "patch": "ticker := time.NewTicker(1*time.Second)\n// missing defer ticker.Stop()", "expected": "FAIL"},
    {"bug": "Array index out of bounds exception during header parsing.", "patch": "if len(parts) > 1 { use(parts[1]) }", "expected": "PASS"},
    {"bug": "Cross-site scripting (XSS) via unescaped template output.", "patch": "return html.Raw(userInput)", "expected": "FAIL"},
    {"bug": "Cross-site scripting mitigation via contextual auto-escaping.", "patch": "return html.EscapeString(userInput)", "expected": "PASS"},
    {"bug": "Potential dead lock due to re-entrant lock acquisition.", "patch": "mu.Lock()\nmu.Lock()", "expected": "FAIL"},
    {"bug": "Divide by zero panic when active node counter drops to zero.", "patch": "if total == 0 { return 0 }\nreturn sum / total", "expected": "PASS"},
    {"bug": "File descriptor leak in configuration reloader loop.", "patch": "f, _ := os.Open(path)\n// missing f.Close()", "expected": "FAIL"},
    {"bug": "Goroutine leak because of sending to an unbuffered channel with no reader.", "patch": "ch := make(chan int)\ngo func() { ch <- 1 }()", "expected": "FAIL"},
    {"bug": "Safe goroutine communication using buffered signal channel.", "patch": "ch := make(chan int, 1)\nch <- 1", "expected": "PASS"},
    {"bug": "Race condition on shared global counter variable.", "patch": "atomic.AddInt64(&counter, 1)", "expected": "PASS"},
    {"bug": "Use of weak cryptographic hash function MD5 for password hashing.", "patch": "h := md5.New()\nh.Write([]byte(pwd))", "expected": "FAIL"}
]

# ==========================================
# 🛠️ 鲁棒解析器：解耦格式干扰，直击判决核心
# ==========================================
def parse_decision_robust(raw: str) -> str:
    raw_upper = raw.upper()
    
    # 1. 优先提取 JSON 结构
    try:
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            obj = json.loads(match.group())
            if "decision" in obj:
                return obj["decision"].upper()
    except:
        pass
    
    # 2. 兜底策略：大白话匹配（剔除因为截断或多余解释导致的语义沉没）
    if "PASS" in raw_upper and "FAIL" not in raw_upper:
        return "PASS"
    if "FAIL" in raw_upper:
        return "FAIL"
        
    return "PARSE_ERROR"

def load_hardware_aligned_model(with_adapter=False):
    """标准的 2026 硬件级硬加载规范"""
    base = AutoModelForCausalLM.from_pretrained(
        MODEL_ID, 
        dtype=torch.float16, 
        device_map={"": 0}
    )
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    if with_adapter:
        model = PeftModel.from_pretrained(base, ADAPTER_DIR)
        print("🟢 已成功挂载本地 LoRA 权重的微调模型。")
    else:
        model = base
        print("🔵 已加载纯净基座模型（无 LoRA）。")
    model.eval()
    return model, tokenizer

def evaluate_model_pipeline(model, tokenizer, is_base_model=False):
    correct_count = 0
    parse_error_count = 0
    results = []

    for idx, case in enumerate(test_cases):
        prompt = (
            f"<|im_start|>system\nYou are a lightweight Critic Agent. Evaluate the patch and return JSON.<|im_end|>\n"
            f"<|im_start|>user\nReview this patch:\n{case['patch']}\nFor bug: {case['bug']}<|im_end|>\n"
            f"<|im_start|>assistant\n"
        )
        inputs = tokenizer(prompt, return_tensors="pt").to("cuda")
        
        with torch.no_grad():
            outputs = model.generate(
                **inputs, 
                max_new_tokens=64, 
                use_cache=True, 
                do_sample=False,
                eos_token_id=tokenizer.eos_token_id,
                pad_token_id=tokenizer.eos_token_id
            )
            
        raw_output = tokenizer.decode(outputs[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
        decision = parse_decision_robust(raw_output)
        
        # 统计解析状态
        if decision == "PARSE_ERROR":
            parse_error_count += 1
            
        is_correct = (decision == case["expected"])
        if is_correct:
            correct_count += 1
            
        results.append({
            "idx": idx + 1,
            "expected": case["expected"],
            "decision": decision,
            "raw": raw_output.strip().replace("\n", " ")
        })
        
    return correct_count, parse_error_count, results

if __name__ == "__main__":
    print("=== 🛠️  Week 8 多智能体 A/B 对撞评测（20条大样本完整版） ===")
    
    # 🧪 Phase 1: 基座模型盲测
    print("\n[Phase 1/2] 正在压测原始基座模型...")
    b_model, b_tok = load_hardware_aligned_model(with_adapter=False)
    b_correct, b_parse_err, b_res = evaluate_model_pipeline(b_model, b_tok, is_base_model=True)
    
    # 物理释放显存，坚决杜绝泄露
    del b_model
    torch.cuda.empty_cache()
    
    # 🧪 Phase 2: 微调适配器盲测
    print("\n[Phase 2/2] 正在压测本地微调模型...")
    ft_model, ft_tok = load_hardware_aligned_model(with_adapter=True)
    ft_correct, ft_parse_err, ft_res = evaluate_model_pipeline(ft_model, ft_tok, is_base_model=False)
    
    # ==========================================
    # 📊 终极硬核战报输出
    # ==========================================
    total = len(test_cases)
    b_acc = (b_correct / total) * 100
    ft_acc = (ft_correct / total) * 100
    
    print("\n" + "="*60)
    print("🎯 【Week 8 核心交付物：20条复杂用例 A/B 对比评测战报】")
    print("="*60)
    print(f"🔵 基座模型 (Base Model)  真实准确率: {b_acc:.1f}% ({b_correct}/{total}) | 格式碎裂数: {b_parse_err}")
    print(f"🟢 微调模型 (Fine-tuned) 真实准确率: {ft_acc:.1f}% ({ft_correct}/{total}) | 格式碎裂数: {ft_parse_err}")
    print(f"🚀 判决能力净增量 (Delta Net Gain):  +{ft_acc - b_acc:.1f}%")
    print("="*60)
    
    print("\n🔍 【诚实诊断：前 3 条测试用例原始输出对撞】")
    for i in range(3):
        print(f"\n📋 [Case #{b_res[i]['idx']}] 期望裁决: {b_res[i]['expected']}")
        print(f"   🔵 基座原始输出 -> 判定: [{b_res[i]['decision']}] | 内容: {b_res[i]['raw'][:100]}...")
        print(f"   🟢 微调原始输出 -> 判定: [{ft_res[i]['decision']}] | 内容: {ft_res[i]['raw'][:100]}...")
