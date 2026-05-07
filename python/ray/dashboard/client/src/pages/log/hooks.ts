import useSWR from "swr";
import {
  getStateApiDownloadLogUrl,
  getStateApiLog,
  LogError,
  StateApiLogInput,
} from "../../service/log";

export const useStateApiLogs = (
  props: StateApiLogInput,
  path: string | undefined,
) => {
  const downloadUrl = getStateApiDownloadLogUrl({ ...props, maxLines: -1 });

  const {
    data: log,
    isLoading,
    mutate,
  } = useSWR(
    downloadUrl ? ["useDriverLogs", downloadUrl] : null,
    async ([_]) => {
      return getStateApiLog(props);
    },
  );

  return {
    log: isLoading ? "Loading..." : (log as string | LogError | undefined),
    downloadUrl,
    refresh: mutate,
    path,
  };
};
