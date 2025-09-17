import json
from pprint import pprint


def main():
    data = []
    with open("failed_traces.json", "r") as f:
        traces = json.load(f)

    for trace in traces:
        if trace['final_status'] != 'success':
            data.append(trace)

    with open("failed_traces_summary.json", "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    main()
