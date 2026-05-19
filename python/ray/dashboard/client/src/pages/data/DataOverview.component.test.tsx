import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import DataOverview from "./DataOverview";

describe("DataOverview", () => {
  it("renders table with dataset metrics", async () => {
    const datasets = [
      {
        dataset: "test_ds1",
        job_id: "test_job_id1",
        state: "RUNNING",
        progress: 50,
        total: 100,
        start_time: 0,
        end_time: undefined,
        ray_data_output_rows: {
          max: 10,
        },
        ray_data_spilled_bytes: {
          max: 20,
        },
        ray_data_current_bytes: {
          value: 30,
          max: 40,
        },
        ray_data_cpu_usage_cores: {
          value: 50,
          max: 60,
        },
        ray_data_gpu_usage_cores: {
          value: 70,
          max: 80,
        },
        num_errored_blocks: 0,
        output_rows: 10,
        input_rows: 12,
        input_blocks: 3,
        queued_blocks: 150,
        operators: [
          {
            operator: "test_ds1_op1",
            name: "test_ds1_op",
            state: "RUNNING",
            progress: 99,
            total: 101,
            queued_blocks: 3,
            ray_data_output_rows: {
              max: 11,
            },
            ray_data_spilled_bytes: {
              max: 21,
            },
            ray_data_current_bytes: {
              value: 31,
              max: 41,
            },
            ray_data_cpu_usage_cores: {
              value: 51,
              max: 61,
            },
            ray_data_gpu_usage_cores: {
              value: 71,
              max: 81,
            },
            num_errored_blocks: 0,
            output_rows: 11,
            input_rows: 13,
            input_blocks: 4,
          },
        ],
      },
      {
        dataset: "test_ds2",
        job_id: "test_job_id2",
        state: "FINISHED",
        progress: 200,
        total: 200,
        start_time: 1,
        end_time: 2,
        ray_data_output_rows: {
          max: 50,
        },
        ray_data_spilled_bytes: {
          max: 60,
        },
        ray_data_current_bytes: {
          value: 70,
          max: 80,
        },
        ray_data_cpu_usage_cores: {
          value: 90,
          max: 100,
        },
        ray_data_gpu_usage_cores: {
          value: 110,
          max: 120,
        },
        num_errored_blocks: 0,
        output_rows: 50,
        input_rows: 55,
        input_blocks: 14,
        queued_blocks: 0,
        operators: [],
      },
    ];
    const user = userEvent.setup();

    render(<DataOverview datasets={datasets} />, { wrapper: TEST_APP_WRAPPER });

    // First Dataset
    expect(screen.getByText("test_ds1")).toBeVisible();
    expect(screen.getByText("50 / 100")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:00")).toBeVisible();
    expect(screen.getByText("10")).toBeVisible();
    expect(screen.getByText("20.0000B")).toBeVisible();
    expect(screen.getByText("30.0000B/40.0000B")).toBeVisible();
    expect(screen.getByText("50/60")).toBeVisible();
    expect(screen.getByText("70/80")).toBeVisible();

    // Operator dropdown
    expect(screen.queryByText("test_ds1_op")).toBeNull();
    await user.click(screen.getByTitle("Expand Dataset test_ds1"));
    expect(screen.getByText("test_ds1_op")).toBeVisible();
    // Verify queued_blocks is rendered for operator row
    expect(screen.getByText("3")).toBeVisible();
    await user.click(screen.getByTitle("Collapse Dataset test_ds1"));
    expect(screen.queryByText("test_ds1_op")).toBeNull();

    // Second Dataset
    expect(screen.getByText("test_ds2")).toBeVisible();
    expect(screen.getByText("200 / 200")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:01")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:02")).toBeVisible();
    expect(screen.getByText("50")).toBeVisible();
    expect(screen.getByText("60.0000B")).toBeVisible();
    expect(screen.getByText("70.0000B/80.0000B")).toBeVisible();
    expect(screen.getByText("90/100")).toBeVisible();
    expect(screen.getByText("110/120")).toBeVisible();
  });

  it("displays errored blocks count with error styling when > 0", async () => {
    const datasets = [
      {
        dataset: "errored_ds",
        job_id: "test_job_id",
        state: "RUNNING",
        progress: 50,
        total: 100,
        start_time: 0,
        end_time: undefined,
        ray_data_output_rows: { max: 10 },
        ray_data_spilled_bytes: { max: 20 },
        ray_data_current_bytes: { value: 30, max: 40 },
        ray_data_cpu_usage_cores: { value: 50, max: 60 },
        ray_data_gpu_usage_cores: { value: 70, max: 80 },
        num_errored_blocks: 5,
        output_rows: 10,
        input_rows: 15,
        input_blocks: 6,
        queued_blocks: 88,
        operators: [
          {
            operator: "errored_op1",
            name: "ErroredOperator",
            state: "RUNNING",
            progress: 45,
            total: 100,
            ray_data_output_rows: { max: 8 },
            ray_data_spilled_bytes: { max: 15 },
            ray_data_current_bytes: { value: 25, max: 35 },
            ray_data_cpu_usage_cores: { value: 40, max: 50 },
            ray_data_gpu_usage_cores: { value: 60, max: 70 },
            num_errored_blocks: 3,
            output_rows: 8,
            input_rows: 11,
            input_blocks: 4,
            queued_blocks: 55,
          },
        ],
      },
    ];
    const user = userEvent.setup();

    render(<DataOverview datasets={datasets} />, { wrapper: TEST_APP_WRAPPER });

    // Dataset level errored blocks should be displayed (value is 5)
    expect(screen.getByText("5")).toBeVisible();

    // Expand to see operator level errored blocks
    await user.click(screen.getByTitle("Expand Dataset errored_ds"));
    expect(screen.getByText("ErroredOperator")).toBeVisible();
    expect(screen.getByText("3")).toBeVisible();
  });
});
