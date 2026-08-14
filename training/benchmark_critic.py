import json
import torch
import time
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
ADAPTER_DIR = "./critic_qlora_local_adapter"

print("🧠 正在使用原生半精度(Float16)无损高速模式加载...")
# 🌟 物理绝杀：移除 quantization_config，直接让模型常驻显存，免去任何解包开销！
base_model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID, 
    torch_dtype=torch.float16, 
    device_map={"": 0}
)
model = PeftModel.from_pretrained(base_model, ADAPTER_DIR)
tokenizer = AutoTokenizer.from_pretrained(ADAPTER_DIR)
model.eval()
# 🌟 准备一个训练集之外的全新 Held-out 测试补丁（用来验证它是不是真懂，还是瞎猜）
unseen_prompt = (
    f"<|im_start|>system\nYou are a lightweight Critic Agent. Evaluate the patch and return JSON.<|im_end|>\n"
    f"<|im_start|>user\nReview this patch:\n"
    f"if node == nil {{ return nil }}\n"
    f"For bug: nil pointer dereference in fetchNodes during cluster lookup.<|im_end|>\n"
    f"<|im_start|>assistant\n"
)

inputs = tokenizer(unseen_prompt, return_tensors="pt").to("cuda")

print("\n🚀 启动硬件级推理压测...")
t0 = time.time()
with torch.no_grad():
    outputs = model.generate(
        **inputs, 
        max_new_tokens=128, 
        temperature=0.1, 
        do_sample=False,
        eos_token_id=tokenizer.eos_token_id
    )
latency = time.time() - t0

response = tokenizer.decode(outputs[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)

print(f"\n⏱️  [RTX 5060 真实推理延迟]: {latency:.4f} 秒")
print("🤖 [小模型纯净吐出的 JSON 决策链]:")
print(response)
