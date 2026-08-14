import torch
import time
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    Trainer,
    DataCollatorForSeq2Seq
)
from peft import LoraConfig, get_peft_model, TaskType

# ==========================================
# 1. 黄金低显存基座拓扑
# ==========================================
MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
DATASET_PATH = "critic_train_data.jsonl"
OUTPUT_DIR = "./critic_qlora_local_adapter"

print("📥 [1/5] 正在热加载 1.5B 分词器...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, trust_remote_code=True)
tokenizer.pad_token = tokenizer.eos_token

# ==========================================
# 2. 物理门禁：4-bit 双重物理量化（极限省显存）
# ==========================================
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,       # 开启双重量化，额外省下几百MB
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16  # 保持计算半精度
)

print("🚀 [2/5] 注入 4-bit 量化物理门禁（锁死基座显存占用）...")
model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID,
    quantization_config=bnb_config,
    device_map={"": 0},                  # 强行锁定单卡 0，避免分布式通信开销
    trust_remote_code=True
)

# 🌟 开启梯度检查点 (Gradient Checkpointing) -> 用计算换空间！
# 这一步能让训练时的激活值显存暴跌 60% 以上！是 8GB 显卡的救命稻草
model.gradient_checkpointing_enable()

# ==========================================
# 3. LoRA 适配器轻量级拦截
# ==========================================
peft_config = LoraConfig(
    task_type=TaskType.CAUSAL_LM,
    r=8,                                 # Rank 缩到 8，足够分类和 CoT 任务
    lora_alpha=16,
    lora_dropout=0.05,
    # 集中拦截核心变换矩阵，省下不必要的显存
    target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
    bias="none"
)
model = get_peft_model(model, peft_config)
model.print_trainable_parameters()

# ==========================================
# 4. 数据流零复制对齐
# ==========================================
print("🥩 [3/5] 加载本地清洗出的高质量粮食...")
dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

def preprocess_function(examples):
    # 将 OpenAI Messages 扁平化拼接为小模型可读的纯文本 tokens
    texts = []
    for msg_list in examples["messages"]:
        flattened = ""
        for msg in msg_list:
            flattened += f"<|im_start|>{msg['role']}\n{msg['content']}<|im_end|>\n"
        texts.append(flattened)
    
    inputs = tokenizer(texts, truncation=True, max_length=512, padding=False)
    inputs["labels"] = inputs["input_ids"].copy() # 语言模型自回归 Target
    return inputs

tokenized_dataset = dataset.map(preprocess_function, batched=True, remove_columns=["messages"])

# ==========================================
# 5. 极致紧缩的训练控制矩阵
# ==========================================
training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=1,       # 🌟 物理 Batch 锁死为 1
    gradient_accumulation_steps=2,       # 🌟 梯度累加 8 次，等效真实 Batch 为 8
    learning_rate=2e-4,
    logging_steps=1,
    num_train_epochs=5,                  # 5 个循环拉满
    fp16=True,                           # 强制半精度反向传播
    # 🌟 核心大招：使用 8-bit Paged AdamW。
    # 一旦 RTX 5060 顶不住了，它会把显存无缝置换到你的系统内存里，绝对不 OOM！
    optim="paged_adamw_8bit",            
    report_to="none",
    save_strategy="no"
)

# 启动底层原生 Trainer
trainer = Trainer(
    model=model,
    train_dataset=tokenized_dataset,
    args=training_args,
    data_collator=DataCollatorForSeq2Seq(tokenizer=tokenizer, pad_to_multiple_of=8, return_tensors="pt")
)

print("🏋️ [4/5] RTX 5060 核心阵列已就位，本地微调全面开火...")
start_time = time.time()
trainer.train()

# ==========================================
# 6. 持久化保存
# ==========================================
print(f"💾 [5/5] 训练完毕，总耗时: {time.time()-start_time:.2f}s. 正在保存本地适配器...")
trainer.model.save_pretrained(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)
print(f"🎉 [Done] 只有几十兆大小的 LoRA 权重已写入: {OUTPUT_DIR}")
