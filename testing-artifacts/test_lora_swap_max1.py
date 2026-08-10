"""LoRA slot-swap validation for the memory-regions backend (max_loras=1).

vLLM holds a single LoRA slot on the GPU; two adapters (A and B) are
alternately snapshotted into and restored from named snapshot slots via the
snapshot agent's MemoryRegionsBackendConfig API. Verifies:
  * restoring a slot hijacks the live adapter (output equality), and
  * the restored LoRA tensor bytes match the snapshotted ones bitwise,
  * across >= SWAP_CYCLES alternating restores.

Ported from the prototype's group-keyed memory_addresses API to the upstream
BackendConfig oneof: slots are named with snapshot_name (NOT the request's
group, which is orchestrator-owned).
"""

import json
import os
import sys

import torch
from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest
from vllm.lora.layers import BaseLayerWithLoRA

# Add path to client library
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../pkg/client/python")))
try:
    from timeslice.snapshot_agent import SnapshotAgentClient, memory_regions_config
except ImportError:
    # Try container path
    sys.path.append("/app/pkg/client/python")
    from timeslice.snapshot_agent import SnapshotAgentClient, memory_regions_config

FAILURES = []


def check(ok, success_msg, failure_msg):
    if ok:
        print(f"SUCCESS: {success_msg}")
    else:
        print(f"FAILURE: {failure_msg}")
        FAILURES.append(failure_msg)


def generate_dummy_lora_from_model(model, lora_dir, adapter_name):
    print(f"Generating dummy LoRA {adapter_name}...")
    os.makedirs(lora_dir, exist_ok=True)
    target_modules = []
    state_dict = {}
    rank = 16
    val_multiplier = 1.0 if adapter_name == "A" else 10.0  # Make them very different

    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            # Map packed modules to their sub-modules for PEFT naming compatibility
            sub_modules = []
            if name.endswith(".qkv_proj"):
                base_name = name[:-8]  # remove "qkv_proj"
                sub_modules = [base_name + "q_proj", base_name + "v_proj"]
            elif name.endswith(".gate_up_proj"):
                base_name = name[:-12]
                sub_modules = [base_name + "gate_proj", base_name + "up_proj"]
            else:
                sub_modules = [name]

            for sub_name in sub_modules:
                suffix = sub_name.split(".")[-1]
                if suffix not in target_modules:
                    target_modules.append(suffix)

                input_dim = getattr(module, "input_size", None)
                output_dim = getattr(module, "output_size", None)
                if input_dim is None or output_dim is None:
                    base_layer = getattr(module, "base_layer", None)
                    if base_layer:
                        input_dim = getattr(base_layer, "input_size", getattr(base_layer, "in_features", None))
                        output_dim = getattr(base_layer, "output_size", getattr(base_layer, "out_features", None))

                if input_dim and output_dim:
                    sub_output_dim = output_dim
                    if name.endswith(".qkv_proj"):
                        sub_output_dim = output_dim // 3
                    elif name.endswith(".gate_up_proj"):
                        sub_output_dim = output_dim // 2

                    key_a = f"base_model.model.{sub_name}.lora_A.weight"
                    key_b = f"base_model.model.{sub_name}.lora_B.weight"
                    # Use constant values to make output deterministic
                    state_dict[key_a] = torch.ones(rank, input_dim) * 0.01 * val_multiplier
                    state_dict[key_b] = torch.ones(sub_output_dim, rank) * 0.01 * val_multiplier

    config = {
        "r": rank,
        "lora_alpha": rank * 2,
        "target_modules": target_modules,
        "lora_dropout": 0.0,
        "bias": "none",
        "task_type": "CAUSAL_LM",
        "peft_type": "LORA",
        "base_model_name_or_path": "dummy",
    }
    with open(os.path.join(lora_dir, "adapter_config.json"), "w") as f:
        json.dump(config, f, indent=2)
    torch.save(state_dict, os.path.join(lora_dir, "adapter_model.bin"))
    print(f"Generated dummy LoRA {adapter_name} at {lora_dir} with target modules {target_modules}")


def _slot_tensors(model, slot_index):
    # Executed on the worker via apply_model.
    import torch
    from vllm.lora.layers import BaseLayerWithLoRA

    tensors = []
    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            lora_a_stacked = getattr(module, "lora_a_stacked", None)
            lora_b_stacked = getattr(module, "lora_b_stacked", None)
            if lora_a_stacked is None or lora_b_stacked is None:
                continue
            if isinstance(lora_a_stacked, tuple):
                for s_index in range(len(lora_a_stacked)):
                    tensors.append(lora_a_stacked[s_index][slot_index])
                    tensors.append(lora_b_stacked[s_index][slot_index])
            elif isinstance(lora_a_stacked, torch.Tensor):
                tensors.append(lora_a_stacked[slot_index])
                tensors.append(lora_b_stacked[slot_index])
    return tensors


def get_lora_regions_from_worker(model, slot_index):
    """Returns legacy 'pid:0xADDR:size' specs for the LoRA slot tensors.

    memory_regions_config() accepts these directly.
    """
    import os

    regions = []
    pid = os.getpid()
    for t in _slot_tensors(model, slot_index):
        size = t.element_size() * t.nelement()
        if size > 0:
            regions.append(f"{pid}:{hex(t.data_ptr())}:{size}")
    return regions


def get_lora_slot_bytes(model, slot_index):
    """Returns the slot tensors' bytes for bitwise comparison."""
    return b"".join(
        t.detach().cpu().contiguous().view(-1).view(torch.uint8).numpy().tobytes()
        for t in _slot_tensors(model, slot_index)
    )


def main():
    # 1. Initialize vLLM with max_loras=1
    model_name = "facebook/opt-125m"
    print(f"Initializing vLLM with model {model_name} (max_loras=1)...")

    # We must set enforce_eager=True and disable_custom_all_reduce=True for CR
    llm = LLM(
        model=model_name,
        enable_lora=True,
        max_loras=1,  # Crucial for testing single-slot swapping
        max_lora_rank=16,
        enforce_eager=True,
        disable_custom_all_reduce=True,
        gpu_memory_utilization=0.4,
    )

    lora_dir_A = "/tmp/lora_A"
    lora_dir_B = "/tmp/lora_B"

    # Generate dummy LoRAs
    def gen_loras(model):
        generate_dummy_lora_from_model(model, lora_dir_A, "A")
        generate_dummy_lora_from_model(model, lora_dir_B, "B")

    llm.llm_engine.apply_model(gen_loras)

    # Connect to Snapshot Agent
    agent_endpoint = os.getenv("AGENT_ENDPOINT", "localhost:9001")
    print(f"Connecting to Snapshot Agent at {agent_endpoint}...")
    try:
        client = SnapshotAgentClient(endpoint=agent_endpoint)
        health = client.check_health("memory-regions")
        print(f"Connected to Snapshot Agent; memory-regions backend: {health.status}")
        if health.status != "SERVING":
            print("memory-regions backend is not SERVING")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to connect to Snapshot Agent: {e}")
        sys.exit(1)

    job_id = os.getenv("JOB_ID", "lora-test-max1")
    # Snapshot slots are named via snapshot_name; the request's group is
    # orchestrator-owned and not set by this test.
    slot_A = os.getenv("SLOT_A", "lora-slot-a")
    slot_B = os.getenv("SLOT_B", "lora-slot-b")
    swap_cycles = int(os.getenv("SWAP_CYCLES", "10"))

    # Since max_loras=1, we only use Slot 0
    slot_index = 0

    def op(kind, slot, regions):
        fn = client.snapshot_and_wait if kind == "snapshot" else client.restore_and_wait
        resp = fn(job_id=job_id, backend_config=memory_regions_config(regions, snapshot_name=slot))
        print(f"{kind} {slot}: {resp.status} ({resp.elapsed_ms}ms)")
        if resp.status != "OPERATION_STATUS_COMPLETE":
            print(f"{kind} of {slot} failed: {resp.error}")
            sys.exit(1)
        return resp

    def slot_bytes():
        return llm.llm_engine.apply_model(lambda m: get_lora_slot_bytes(m, slot_index))[0]

    # 2. Load LoRA A and run inference (loads into Slot 0)
    print("\n--- Loading LoRA A ---")
    lora_request_A = LoRARequest("lora_A", 1, lora_dir_A)
    sampling_params = SamplingParams(temperature=0.0, max_tokens=10)

    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_initial = outputs[0].outputs[0].text
    print(f"LoRA A Initial Output: {repr(output_A_initial)}")

    # Find Slot 0 regions
    targets_A = llm.llm_engine.apply_model(lambda m: get_lora_regions_from_worker(m, slot_index))[0]
    print(f"LoRA A (Slot {slot_index}) targets: {len(targets_A)} regions found.")
    bytes_A = slot_bytes()

    # 3. Snapshot LoRA A (Slot 0)
    print("\n--- Snapshotting LoRA A (Slot 0) ---")
    op("snapshot", slot_A, targets_A)

    print("\n--- Restoring LoRA A (to remap memory for B) ---")
    op("restore", slot_A, targets_A)

    # 4. Load LoRA B and run inference (overwrites Slot 0)
    print("\n--- Loading LoRA B ---")
    lora_request_B = LoRARequest("lora_B", 2, lora_dir_B)
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_B_initial = outputs[0].outputs[0].text
    print(f"LoRA B Initial Output: {repr(output_B_initial)}")

    # Find Slot 0 regions again (should be same as targets_A)
    targets_B = llm.llm_engine.apply_model(lambda m: get_lora_regions_from_worker(m, slot_index))[0]
    print(f"LoRA B (Slot {slot_index}) targets: {len(targets_B)} regions found.")
    bytes_B = slot_bytes()

    # Verify targets are identical (proving they share the slot)
    check(
        targets_A == targets_B,
        "Slot addresses for A and B are identical.",
        "Targets for A and B are different! Slot addresses are not static?",
    )

    # 5. Snapshot LoRA B (Slot 0)
    print("\n--- Snapshotting LoRA B (Slot 0) ---")
    op("snapshot", slot_B, targets_B)

    # 6. Restore A and test (Hijack Test)
    # vLLM thinks B is active in Slot 0. We restore A's weights into Slot 0.
    # If we request B, we should get A's output.
    print("\n--- Restoring LoRA A (Slot 0) ---")
    op("restore", slot_A, targets_A)

    check(
        slot_bytes() == bytes_A,
        "LoRA A slot bytes bitwise-identical after restore.",
        "LoRA A slot bytes differ from snapshot (bitwise mismatch).",
    )

    print("Running query requesting LoRA B (expecting LoRA A output)...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_after_restore_A = outputs[0].outputs[0].text
    print(f"Output (requested B): {repr(output_after_restore_A)}")
    check(
        output_after_restore_A == output_A_initial,
        "LoRA A restored correctly (hijacked LoRA B request)!",
        f"LoRA A restored output mismatch! Expected {repr(output_A_initial)}, got {repr(output_after_restore_A)}",
    )

    # 7. Restore B and test
    print("\n--- Restoring LoRA B (Slot 0) ---")
    op("restore", slot_B, targets_B)

    check(
        slot_bytes() == bytes_B,
        "LoRA B slot bytes bitwise-identical after restore.",
        "LoRA B slot bytes differ from snapshot (bitwise mismatch).",
    )

    print("Running query requesting LoRA B (expecting LoRA B output)...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_after_restore_B = outputs[0].outputs[0].text
    print(f"Output (requested B): {repr(output_after_restore_B)}")
    check(
        output_after_restore_B == output_B_initial,
        "LoRA B restored correctly!",
        f"LoRA B restored output mismatch! Expected {repr(output_B_initial)}, got {repr(output_after_restore_B)}",
    )

    # 8. Alternate restores >= swap_cycles times; verify outputs and slot
    # bytes each cycle. First-token behavior of the "other tenant" after a
    # swap is exactly the 2026-08-04 co-resident-corruption regression check.
    print(f"\n--- Alternating slot restores x{swap_cycles} ---")
    for cycle in range(swap_cycles):
        slot, targets, expect_bytes, expect_out = (
            (slot_A, targets_A, bytes_A, output_A_initial)
            if cycle % 2 == 0
            else (slot_B, targets_B, bytes_B, output_B_initial)
        )
        resp = op("restore", slot, targets)
        check(
            slot_bytes() == expect_bytes,
            f"cycle {cycle}: {slot} bytes bitwise-identical.",
            f"cycle {cycle}: {slot} bytes mismatch.",
        )
        outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
        got = outputs[0].outputs[0].text
        check(
            got == expect_out,
            f"cycle {cycle}: output matches {slot} baseline.",
            f"cycle {cycle}: output {repr(got)} != {slot} baseline {repr(expect_out)}",
        )

    # 9. Requesting A triggers a vLLM reload and must still work.
    print("\n--- Requesting LoRA A (triggers vLLM reload) ---")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_reloaded = outputs[0].outputs[0].text
    print(f"Output (requested A): {repr(output_A_reloaded)}")
    check(
        output_A_reloaded == output_A_initial,
        "LoRA A reloaded and worked correctly.",
        "LoRA A reload failed.",
    )

    if FAILURES:
        print(f"\nLoRA Swap Max 1 Test FAILED ({len(FAILURES)} failures):")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("\nLoRA Swap Max 1 Test PASSED.")


if __name__ == "__main__":
    main()
