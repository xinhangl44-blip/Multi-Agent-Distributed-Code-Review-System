import requests

EVAL_DATASET = [
    {"query": "function that creates pods for a GPU job", "expected_function": "buildPodForJob"},
    {"query": "reconcile GPUJob and create missing pods", "expected_function": "Reconcile"},
    {"query": "detect deadlock among waiting gang jobs", "expected_function": "reconcileDeadlock"},
    {"query": "fetch GPU jobs from Kubernetes API", "expected_function": "fetchGPUJobs"},
    {"query": "fetch cluster nodes with GPU layout annotation", "expected_function": "fetchNodes"},
    {"query": "convert bytes to mebibytes", "expected_function": "toMiB"},
    {"query": "estimate wait time based on job priority", "expected_function": "estimateWait"},
    {"query": "release reserved VRAM after a failed gang assembly", "expected_function": "releaseVRAM"},
    {"query": "how does the scheduler decide which pod gets evicted when two gangs are stuck waiting on each other", "expected_function": "reconcileDeadlock"},
    {"query": "logic for picking the best GPU to place a pod onto based on remaining free memory", "expected_function": "tryAssign"},
    {"query": "what happens when a gang of pods times out before fully assembling", "expected_function": "timeoutGang"},
    {"query": "comparator used to order pending pods in the scheduling queue", "expected_function": "Less"},
    {"query": "code that builds a Kubernetes client from local kubeconfig", "expected_function": "newK8sClient"},
    {"query": "summary statistics about total and used GPU memory across the cluster", "expected_function": "clusterInfo"},
    {"query": "assign a priority class name based on a numeric priority value", "expected_function": "priorityClassForValue"},
]
SERVICE_URL = "http://127.0.0.1:8000/retrieve"

def evaluate_recall(k: int):
    hit_count = 0
    misses = []
    for item in EVAL_DATASET:
        payload = {"query": item["query"], "limit": k}
        try:
            response = requests.post(SERVICE_URL, json=payload).json()
            retrieved_funcs = [point["function_name"] for point in response.get("data", [])]
            if item["expected_function"] in retrieved_funcs:
                hit_count += 1
            else:
                misses.append((item["query"], item["expected_function"], retrieved_funcs))
        except Exception as e:
            print(f"Evaluation request failed: {e}")
    recall_score = hit_count / len(EVAL_DATASET)
    print(f"\nRecall@{k} Evaluation Score: {recall_score * 100:.2f}% (Hit {hit_count}/{len(EVAL_DATASET)})")
    if misses:
        print("  Miss details:")
        for query, expected, got in misses:
            print(f"    - Expected '{expected}', got {got}, query: \"{query}\"")
    return recall_score

if __name__ == "__main__":
    evaluate_recall(k=5)
    evaluate_recall(k=10)
    metrics = requests.get("http://127.0.0.1:8000/metrics").json()
    print(f"\nLatency Baseline Metrics: {metrics}")
