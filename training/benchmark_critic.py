import json
import torch
import time
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
ADAPTER_DIR = "./critic_qlora_local_adapter"
base_model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID, 
    torch_dtype=torch.float16, 
    device_map={"": 0}
)
model = PeftModel.from_pretrained(base_model, ADAPTER_DIR)
tokenizer = AutoTokenizer.from_pretrained(ADAPTER_DIR)
model.eval()
unseen_prompt = (
    f"<|im_start|>system\nYou are a lightweight Critic Agent. Evaluate the patch and return JSON.<|im_end|>\n"
    f"<|im_start|>user\nReview this patch:\n"
    f"if node == nil {{ return nil }}\n"
    f"For bug: nil pointer dereference in fetchNodes during cluster lookup.<|im_end|>\n"
    f"<|im_start|>assistant\n"
)
inputs = tokenizer(unseen_prompt, return_tensors="pt").to("cuda")
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
print(f"Latency: {latency:.4f}s")
print(response)
