import sys
import json


def generate_input_paths(step_name, input_paths, task_ids_json):
    """
    Generate input paths from collected task IDs.

    Args:
        step_name: Name of the foreach step (e.g., "a")
        input_paths: Base input paths (e.g., "kfp-run/start/task-xyz")
        task_ids_json: JSON array of task IDs (e.g., '["task-123", "task-456"]')

    Returns:
        Formatted pathspec: "run_id/step/:task-123,task-456"
    """
    run_id = input_paths.split("/")[0]
    ids = json.loads(task_ids_json)
    return "{}/{}/:{}".format(run_id, step_name, ",".join(ids))


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1], sys.argv[2], sys.argv[3]))
