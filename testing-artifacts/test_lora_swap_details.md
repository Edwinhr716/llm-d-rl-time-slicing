# Detailed Explanation of `test_lora_swap.py`

This document explains the purpose and inner workings of the [test_lora_swap.py](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/test_lora_swap.py) script in simple terms.

## High-Level Summary

The `test_lora_swap.py` script is a test case designed to verify a **Checkpoint/Restore (CR)** mechanism for **LoRA (Low-Rank Adaptation)** weights in vLLM. 

Instead of reloading LoRA adapters from disk (which can be slow), this script tests if we can save (snapshot) the raw GPU memory where a LoRA is loaded, and later restore that memory directly to swap the active LoRA adapter.

---

## Key Concepts

Before diving into the code, here are the key concepts:

*   **vLLM:** A high-performance engine for serving Large Language Models (LLMs).
*   **LoRA (Low-Rank Adaptation):** A technique to customize a base LLM for specific tasks by adding small, trainable layers (adapters). vLLM allows loading multiple LoRAs at the same time.
*   **LoRA Slots:** vLLM allocates space in GPU memory for a fixed number of LoRAs. These are like "slots". When you load a LoRA, it goes into one of these slots.
*   **Snapshot Agent:** An external service (likely part of the infrastructure this test runs in) that can copy specific regions of GPU memory to a backup (snapshot) and write them back (restore).
*   **GPU CR (Checkpoint/Restore):** The process of saving the state of GPU memory and restoring it later.

---

## Detailed Step-by-Step Walkthrough

The script performs the following steps:

### 1. Initialization and Setup
*   **Starts vLLM:** It initializes the vLLM engine with a small model (`facebook/opt-125m`). It configures it to support up to 2 LoRAs with a maximum rank of 16. It also sets options like `enforce_eager=True` which are necessary for memory tracking.
*   **Generates Dummy LoRAs:** It programmatically creates two different dummy LoRA adapters, "LoRA A" and "LoRA B", and saves them to `/tmp/lora_A` and `/tmp/lora_B`.
    *   **LoRA A** is generated with small weights.
    *   **LoRA B** is generated with weights that are 10 times larger.
    *   This difference ensures they will produce visibly different outputs when used with the model.
*   **Connects to Snapshot Agent:** It connects to the Snapshot Agent service running on the host (usually `localhost:9001`).

### 2. Loading and Profiling LoRA A
*   **Loads LoRA A:** It requests vLLM to generate text using "LoRA A". vLLM loads LoRA A into **Slot 0** in GPU memory.
*   **Captures Initial Output:** It prompts the model with "Who are you?" and saves the output (`output_A_initial`).
*   **Finds Memory Addresses:** It queries the vLLM worker to find the exact GPU memory addresses (pointers and sizes) where LoRA A (Slot 0) is stored.

### 3. Loading and Profiling LoRA B
*   **Loads LoRA B:** It requests vLLM to generate text using "LoRA B". vLLM loads LoRA B into **Slot 1** in GPU memory.
*   **Captures Initial Output:** It prompts the model with "Who are you?" and saves the output (`output_B_initial`).
*   **Finds Memory Addresses:** It finds the exact GPU memory addresses where LoRA B (Slot 1) is stored.
*   **Verifies Difference:** It checks that the initial outputs of A and B are indeed different, confirming the dummy LoRAs are working.

### 4. Snapshotting
*   **Snapshots LoRA A:** It tells the Snapshot Agent to save the GPU memory regions identified for LoRA A (Slot 0).
*   **Restores LoRA A (Intermediate):** It performs a restore of LoRA A. (This might be to reset some state or verify the restore path works immediately).
*   **Snapshots LoRA B:** It tells the Snapshot Agent to save the GPU memory regions identified for LoRA B (Slot 1).

### 5. Verification of Restore (Swapping)
Now that both LoRAs are snapshotted, the script verifies it can swap them by restoring their memory.

*   **Restores LoRA A:**
    1.  It calls the Snapshot Agent to restore the saved memory for LoRA A (Slot 0). This overwrites the current GPU memory in Slot 0 with the saved state.
    2.  It runs the prompt "Who are you?" requesting LoRA A.
    3.  It verifies that the output matches `output_A_initial`. If it matches, it means LoRA A was successfully restored.

*   **Restores LoRA B:**
    1.  It calls the Snapshot Agent to restore the saved memory for LoRA B (Slot 1).
    2.  It runs the prompt "Who are you?" requesting LoRA B.
    3.  It verifies that the output matches `output_B_initial`. If it matches, it means LoRA B was successfully restored.

---

## Deep Dive: LoRA Generation & Memory Fetching

### How LoRA Adapters are Generated

The script generates dummy LoRA adapters programmatically in the `generate_dummy_lora_from_model` function. This avoids needing pre-trained adapter files.

1.  **Iterating Model Layers:** The script inspects the base LLM structure and looks for layers that support LoRA (specifically, instances of `BaseLayerWithLoRA`).
2.  **Identifying Target Modules:** It identifies which modules inside those layers should be targeted (e.g., `q_proj`, `v_proj` for attention layers). It handles "packed" layers (like `qkv_proj`) by mapping them to their individual constituent projections.
3.  **Determining Dimensions:** For each target module, it finds the input and output dimensions. This is crucial for creating weights of the correct shape.
4.  **Creating Dummy Weights:** It creates weight tensors filled with constant values (all ones) scaled by a factor.
    *   **LoRA A** weights are scaled by `0.01`.
    *   **LoRA B** weights are scaled by `0.1` (10x larger).
    *   This scaling difference ensures the two adapters produce different outputs, allowing the test to verify that the swap actually happened.
5.  **Saving PEFT Structure:** It writes these weights into `adapter_model.bin` and a standard PEFT configuration into `adapter_config.json` in the respective temp directories (`/tmp/lora_A` and `/tmp/lora_B`). This makes them look like standard Hugging Face PEFT adapters to vLLM.

### How GPU Memory Addresses are Fetched

To snapshot the LoRAs, the script needs to know exactly where they reside in GPU memory. This is handled by `get_lora_addresses_from_worker`, which runs on the vLLM worker process.

1.  **Accessing Stacked Tensors:** vLLM optimizes multi-LoRA execution by "stacking" the weights of different LoRAs into single, larger tensors. Each LoRA is assigned a "slot index" (e.g., Slot 0 for LoRA A, Slot 1 for LoRA B).
2.  **Slicing for the Slot:** The script iterates through the model's layers and finds the stacked LoRA tensors (`lora_a_stacked` and `lora_b_stacked`). It then slices these tensors using the target `slot_index` to isolate the memory for the specific LoRA we want to snapshot.
3.  **Extracting Pointers and Sizes:**
    *   **Pointer:** It uses PyTorch's `.data_ptr()` on the sliced tensor to get the raw GPU memory address.
    *   **Size:** It calculates the size of this tensor in bytes by multiplying the number of elements (`.nelement()`) by the size of each element (`.element_size()`).
4.  **Reporting:** It gathers these address and size pairs for all LoRA-enabled layers in the model and returns them to the main script. The format returned is `process_id:memory_address:size_in_bytes`.

---

## Why is this important?

If this test passes, it proves that:
1.  We can identify the exact GPU memory locations where vLLM stores LoRA weights.
2.  We can successfully copy these memory regions to a backup (snapshot) while the model is running.
3.  We can restore these memory regions, and vLLM will correctly use the restored weights without needing to reload the model or the adapter from scratch.

This technique can enable very fast swapping of customized models (LoRAs) in a serving environment.
