import { ContentCopy } from "@mui/icons-material";
import { alpha, Box, IconButton, Snackbar, Tooltip, Typography } from "@mui/material";
import dayjs from "dayjs";
import prolog from "highlight.js/lib/languages/prolog";
import { lowlight } from "lowlight";
import React, {
  forwardRef,
  MutableRefObject,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import { AutoSizer } from "react-virtualized-auto-sizer";
import { FixedSizeList as List } from "react-window";
import DialogWithTitle from "../../common/DialogWithTitle";
import "./darcula.css";
import "./github.css";
import "./index.css";
import { MAX_LINES_FOR_LOGS } from "../../service/log";

lowlight.registerLanguage("prolog", prolog);

const uniqueKeySelector = () => Math.random().toString(16).slice(-8);

const timeReg =
  /(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)\s+([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/;

const value2react = (
  { type, tagName, properties, children, value = "" }: any,
  key: string,
  keywords = "",
) => {
  switch (type) {
    case "element":
      return React.createElement(
        tagName,
        {
          className: properties.className[0],
          key: `${key}line${uniqueKeySelector()}`,
        },
        children.map((e: any, i: number) =>
          value2react(e, `${key}-${i}`, keywords),
        ),
      );
    case "text":
      if (keywords && value.includes(keywords)) {
        const afterChildren = [];
        const vals = value.split(keywords);
        let tmp = vals.shift();
        if (!tmp) {
          return React.createElement(
            "span",
            { className: "find-kws" },
            keywords,
          );
        }
        while (typeof tmp === "string") {
          if (tmp !== "") {
            afterChildren.push(tmp);
          } else {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }

          tmp = vals.shift();
          if (tmp) {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }
        }
        return afterChildren;
      }
      return value;
    default:
      return [];
  }
};

export type LogVirtualViewProps = {
  content: string;
  width?: number;
  height?: number;
  /** Use AutoSizer to fill available height. When true, height prop is ignored. */
  autoHeight?: boolean;
  /** Minimum height when using autoHeight */
  minHeight?: number;
  fontSize?: number;
  theme?: "light" | "dark";
  language?: string;
  focusLine?: number;
  keywords?: string;
  style?: { [key: string]: string | number };
  listRef?: MutableRefObject<HTMLDivElement | null>;
  onScrollBottom?: (event: Event) => void;
  revert?: boolean;
  startTime?: string;
  endTime?: string;
  searchMode?: "filter" | "locate";
  onMatchInfoChange?: (info: { total: number; currentIndex: number }) => void;
  /** Show copy all button */
  showCopyButton?: boolean;
};

export type LogVirtualViewHandle = {
  scrollToMatch: (matchIndex: number) => void;
};

type LogLineDetailDialogProps = {
  formattedLogLine: string | null;
  message: string;
  onClose: () => void;
};

const LogLineDetailDialog = ({
  formattedLogLine,
  message,
  onClose,
}: LogLineDetailDialogProps) => {
  return (
    <DialogWithTitle title="Log line details" handleClose={onClose}>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          gap: 4,
          alignItems: "stretch",
        }}
      >
        <Box
          sx={{
            width: "100%",
          }}
        >
          {formattedLogLine !== null && (
            <React.Fragment>
              <Typography
                variant="h5"
                sx={{
                  marginBottom: 2,
                }}
              >
                Raw log line
              </Typography>
              <Box
                sx={(theme) => ({
                  padding: 1,
                  bgcolor:
                    theme.palette.mode === "dark"
                      ? theme.palette.grey[900]
                      : theme.palette.grey[200],
                  borderRadius: 1,
                  border: `1px solid ${theme.palette.divider}`,
                  marginBottom: 2,
                })}
              >
                <Typography
                  component="pre"
                  variant="body2"
                  sx={{
                    whiteSpace: "pre",
                    overflow: "auto",
                    height: "300px",
                  }}
                  data-testid="raw-log-line"
                >
                  {formattedLogLine}
                </Typography>
              </Box>
            </React.Fragment>
          )}
          <Typography
            variant="h5"
            sx={{
              marginBottom: 2,
            }}
          >
            Formatted message
          </Typography>
          <Box
            sx={(theme) => ({
              padding: 1,
              bgcolor:
                theme.palette.mode === "dark"
                  ? theme.palette.grey[900]
                  : theme.palette.grey[200],
              borderRadius: 1,
              border: `1px solid ${theme.palette.divider}`,
            })}
          >
            <Typography
              component="pre"
              variant="body2"
              sx={{
                whiteSpace: "pre",
                overflow: "auto",
                height: "300px",
              }}
              data-testid="raw-log-line"
            >
              {message}
            </Typography>
          </Box>
        </Box>
      </Box>
    </DialogWithTitle>
  );
};

const LogVirtualView = forwardRef<LogVirtualViewHandle, LogVirtualViewProps>(
  (
    {
      content,
      width = "100%",
      height,
      autoHeight = false,
      minHeight = 400,
      fontSize = 12,
      theme = "light",
      keywords = "",
      language = "dos",
      focusLine = 1,
      style = {},
      listRef,
      onScrollBottom,
      revert = false,
      startTime,
      endTime,
      searchMode = "locate",
      onMatchInfoChange,
      showCopyButton = true,
    },
    ref,
  ) => {
    const [logs, setLogs] = useState<{ i: number; origin: string }[]>([]);
    const [matchIndices, setMatchIndices] = useState<number[]>([]);
    const [currentMatchIndex, setCurrentMatchIndex] = useState<number>(-1);
    const [copySnackbarOpen, setCopySnackbarOpen] = useState(false);
    const total = logs.length;
    const timer = useRef<ReturnType<typeof setTimeout>>();
    const el = useRef<List>(null);
    const outter = useRef<HTMLDivElement>(null);
    if (listRef) {
      listRef.current = outter.current;
    }
    const [selectedLogLine, setSelectedLogLine] =
      useState<[string | null, string]>();
    const handleLogLineClick = useCallback(
      (logLine: string | null, message: string) => {
        setSelectedLogLine([logLine, message]);
      },
      [],
    );

    const handleCopyAll = useCallback(() => {
      const textToCopy = logs.map((log) => log.origin).join("\n");
      navigator.clipboard.writeText(textToCopy).then(() => {
        setCopySnackbarOpen(true);
      });
    }, [logs]);

    // Create a set of matching line indices for O(1) lookup
    const matchIndexSet = useMemo(
      () => new Set(matchIndices),
      [matchIndices],
    );

    // Expose scrollToMatch method to parent
    useImperativeHandle(ref, () => ({
      scrollToMatch: (matchIndex: number) => {
        if (matchIndex >= 0 && matchIndex < matchIndices.length) {
          setCurrentMatchIndex(matchIndex);
          const logIndex = matchIndices[matchIndex];
          if (el.current) {
            const listIndex = revert ? logs.length - 1 - logIndex : logIndex;
            el.current.scrollToItem(listIndex, "center");
          }
        }
      },
    }));

    const itemRenderer = ({ index, style: itemStyle }: { index: number; style: any }) => {
      const logIndex = revert ? logs.length - 1 - index : index;
      const { i, origin } = logs[logIndex];
      const isMatch = searchMode === "locate" && keywords && matchIndexSet.has(logIndex);
      const isCurrentMatch = isMatch && matchIndices[currentMatchIndex] === logIndex;

      const getBackgroundColor = (themeArg: any): string => {
        if (isCurrentMatch) {
          return alpha(themeArg.palette.warning.main, 0.2);
        }
        if (isMatch) {
          return alpha(themeArg.palette.warning.main, 0.05);
        }
        return "transparent";
      };

      let message = origin;
      let formattedLogLine: string | null = null;
      try {
        const parsedOrigin = JSON.parse(origin);
        if (parsedOrigin.message) {
          message = parsedOrigin.message;
          if (parsedOrigin.levelname) {
            message = `${parsedOrigin.levelname} ${message}`;
          }
          if (parsedOrigin.asctime) {
            message = `${parsedOrigin.asctime}\t${message}`;
          }
        }
        formattedLogLine = JSON.stringify(parsedOrigin, null, 2);
      } catch {
        // Keep origin as message if JSON parsing failed.
        // formattedLogLine remains null, so we won't show raw JSON dialog.
      }

      return (
        <Box
          key={`${index}list`}
          style={itemStyle}
          sx={(themeArg) => ({
            zIndex: 0,
            overflowX: "visible",
            whiteSpace: "nowrap",
            backgroundColor: getBackgroundColor(themeArg),
            "&:hover .log-line-details-btn": {
              opacity: 1,
              pointerEvents: "auto",
            },
            "&::after": {
              content: '""',
              position: "absolute",
              top: 0,
              right: "calc(-1 * var(--log-view-scroll-left, 0px))",
              width: "var(--log-view-scroll-left, 0px)",
              height: "100%",
              zIndex: -1,
            },
            "&::before": {
              content: `"${i + 1}"`,
              marginRight: 0.5,
              width: `${logs.length}`.length * 6 + 4,
              color: themeArg.palette.text.disabled,
              display: "inline-block",
            },
          })}
        >
          {lowlight
            .highlight(language, message)
            .children.map((v) => value2react(v, index.toString(), keywords))}
          {formattedLogLine !== null && (
            <button
              className="log-line-details-btn"
              onClick={(e) => {
                e.stopPropagation();
                if ((window.getSelection()?.toString().length ?? 0) === 0) {
                  handleLogLineClick(formattedLogLine, message);
                }
              }}
            >
              Show details
            </button>
          )}
          <br />
        </Box>
      );
    };

    useEffect(() => {
      const originContent = content.split("\n");
      if (timer.current) {
        clearTimeout(timer.current);
      }
      timer.current = setTimeout(() => {
        const allLines = originContent.map((e, i) => ({
          i,
          origin: e,
          time: (startTime || endTime) ? (e?.match(timeReg) || [""])[0] : "",
        }));

        // Apply time filtering (always applied regardless of mode)
        const timeFilteredLogs = allLines.filter((e) => {
          if (!e.time) {
            return true;
          }
          const logTime = dayjs(e.time);
          if (startTime && !logTime.isAfter(dayjs(startTime))) {
            return false;
          }
          if (endTime && !logTime.isBefore(dayjs(endTime))) {
            return false;
          }
          return true;
        });

        if (searchMode === "filter" && keywords) {
          // Filter mode: only show lines containing keywords, no matchIndices needed
          const filtered = timeFilteredLogs
            .filter((e) => e.origin.includes(keywords))
            .map((e) => ({ i: e.i, origin: e.origin }));
          setLogs(filtered);
          setMatchIndices([]);
          setCurrentMatchIndex(-1);
        } else {
          // Locate mode (or no keywords): show all lines
          const filtered = timeFilteredLogs.map((e) => ({ i: e.i, origin: e.origin }));
          setLogs(filtered);

          if (keywords) {
            const indices: number[] = [];
            filtered.forEach((line, idx) => {
              if (line.origin.includes(keywords)) {
                indices.push(idx);
              }
            });
            setMatchIndices(indices);
            setCurrentMatchIndex(indices.length > 0 ? 0 : -1);
            // Auto-scroll to first match
            if (indices.length > 0 && el.current) {
              const firstMatchIndex = indices[0];
              setTimeout(() => {
                const listIndex = revert ? filtered.length - 1 - firstMatchIndex : firstMatchIndex;
                el.current?.scrollToItem(listIndex, "center");
              }, 0);
            }
          } else {
            setMatchIndices([]);
            setCurrentMatchIndex(-1);
          }
        }
      }, 500);
    }, [content, keywords, language, startTime, endTime, searchMode, revert]);

    // Notify parent of match info changes
    useEffect(() => {
      if (onMatchInfoChange) {
        onMatchInfoChange({
          total: matchIndices.length,
          currentIndex: currentMatchIndex,
        });
      }
    }, [matchIndices, currentMatchIndex, onMatchInfoChange]);


    useEffect(() => {
      if (el.current) {
        el.current?.scrollTo((focusLine - 1) * (fontSize + 6));
      }
    }, [focusLine, fontSize]);

    useEffect(() => {
      let outterCurrentValue: any = null;
      if (outter.current) {
        outter.current.style.setProperty("--log-view-scroll-left", "0px");

        const scrollFunc = (event: any) => {
          const { target } = event;
          if (target) {
            const scrollLeft = `${target.scrollLeft ?? 0}px`;
            if (
              target.style.getPropertyValue("--log-view-scroll-left") !==
              scrollLeft
            ) {
              target.style.setProperty("--log-view-scroll-left", scrollLeft);
            }
          }
          if (
            target &&
            target.scrollTop + target.clientHeight === target.scrollHeight
          ) {
            if (onScrollBottom) {
              onScrollBottom(event);
            }
          }
          outterCurrentValue = outter.current;
        };
        outter.current.addEventListener("scroll", scrollFunc);
        return () => {
          if (outterCurrentValue) {
            outterCurrentValue.removeEventListener("scroll", scrollFunc);
          }
        };
      }
    }, [onScrollBottom]);

    const renderList = (listHeight: number, listWidth: number | string) => (
      <List
        height={listHeight}
        width={listWidth}
        ref={el}
        outerRef={outter}
        className={`hljs-${theme}`}
        style={{
          fontSize,
          fontFamily: "menlo, monospace",
          ...style,
        }}
        itemSize={fontSize + 6}
        itemCount={total}
        overscanCount={50}
      >
        {itemRenderer}
      </List>
    );

    const showTruncationWarning = logs.length > MAX_LINES_FOR_LOGS;
    const showToolbar = showTruncationWarning || (showCopyButton && logs.length > 0);

    return (
      <div style={{ height: autoHeight ? "100%" : "auto", display: "flex", flexDirection: "column" }}>
        {showToolbar && (
          <Box sx={{ display: "flex", alignItems: "center", gap: 1, marginBottom: 0.5 }}>
            {showTruncationWarning && (
              <Typography component="span" sx={{ color: "error.main", fontSize: 12 }}>
                [Truncation warning] Only the latest {MAX_LINES_FOR_LOGS} lines are displayed.
              </Typography>
            )}
            {showCopyButton && logs.length > 0 && (
              <Tooltip title="Copy all visible logs to clipboard">
                <IconButton size="small" onClick={handleCopyAll}>
                  <ContentCopy fontSize="small" />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        )}
        {autoHeight ? (
          <div style={{ flex: 1, minHeight }}>
            <AutoSizer
              renderProp={({ height: autoH, width: autoW }: { height: number | undefined; width: number | undefined }) =>
                renderList(autoH || minHeight, autoW || "100%")
              }
            />
          </div>
        ) : (
          renderList(height || 600, width)
        )}
        {selectedLogLine && (
          <LogLineDetailDialog
            formattedLogLine={selectedLogLine[0]}
            message={selectedLogLine[1]}
            onClose={() => setSelectedLogLine(undefined)}
          />
        )}
        <Snackbar
          open={copySnackbarOpen}
          autoHideDuration={2000}
          onClose={() => setCopySnackbarOpen(false)}
          message="Copied to clipboard"
        />
      </div>
    );
  },
);

export default LogVirtualView;
