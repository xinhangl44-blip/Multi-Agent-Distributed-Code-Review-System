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

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
DATASET_PATH = "critic_train_data.jsonl"
OUTPUT_DIR = "./critic_qlora_local_adapter"
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, trust_remote_code=True)
tokenizer.pad_token = tokenizer.eos_token
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16
)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID,
    quantization_config=bnb_config,
    device_map={"": 0},
    trust_remote_code=True
)
model.gradient_checkpointing_enable()
peft_config = LoraConfig(
    task_type=TaskType.CAUSAL_LM,
    r=8,
    lora_alpha=16,
    lora_dropout=0.05,
    target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
    bias="none"
)
model = get_peft_model(model, peft_config)
dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

def preprocess_function(examples):
    texts = []
    for msg_list in examples["messages"]:
        flattened = ""
        for msg in msg_list:
            flattened += f"<|im_start|>{msg['role']}\n{msg['content']}<|im_end|>\n"
        texts.append(flattened)
    
    inputs = tokenizer(texts, truncation=True, max_length=512, padding=False)
    inputs["labels"] = inputs["input_ids"].copy()
    return inputs
tokenized_dataset = dataset.map(preprocess_function, batched=True, remove_columns=["messages"])
training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=1,
    gradient_accumulation_steps=2,
    learning_rate=2e-4,
    logging_steps=10,
    num_train_epochs=5,
    fp16=True,
    optim="paged_adamw_8bit",
    report_to="none",
    save_strategy="no"
)
trainer = Trainer(
    model=model,
    train_dataset=tokenized_dataset,
    args=training_args,
    data_collator=DataCollatorForSeq2Seq(tokenizer=tokenizer, pad_to_multiple_of=8, return_tensors="pt")
)
trainer.train()
trainer.model.save_pretrained(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)
