import { useState } from "react";
import { ThumbsDown, ThumbsUp } from "lucide-react";
import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/Tooltip";

export type FeedbackValue = "up" | "down" | null;

interface QueryFeedbackProps {
  value?: FeedbackValue;
  onChange?: (value: FeedbackValue, comment?: string) => void;
  className?: string;
}

// Thumbs up/down + optional comment input. Comment box only appears
// after a thumbs-down so the happy-path stays one click.
//
// Backend doesn't have a feedback endpoint yet — the consumer is
// expected to persist this locally (via the same conversation store
// that holds messages) until the API arrives. The component itself is
// stateless w.r.t. persistence; it only owns the comment-input UI.
export function QueryFeedback({
  value = null,
  onChange,
  className,
}: QueryFeedbackProps) {
  const [showComment, setShowComment] = useState(false);
  const [comment, setComment] = useState("");

  function pick(next: "up" | "down") {
    const newValue = value === next ? null : next;
    if (newValue === "down") {
      setShowComment(true);
    } else {
      setShowComment(false);
      setComment("");
    }
    onChange?.(newValue);
  }

  function submitComment() {
    onChange?.("down", comment.trim() || undefined);
    setShowComment(false);
  }

  return (
    <div className={cn("flex flex-col gap-2", className)}>
      <div className="flex items-center gap-1">
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={() => pick("up")}
              aria-label="Mark as helpful"
              aria-pressed={value === "up"}
              className={cn(
                value === "up" &&
                  "bg-success/10 text-success hover:bg-success/20",
              )}
            >
              <ThumbsUp className="size-3.5" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>This was helpful</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={() => pick("down")}
              aria-label="Mark as inaccurate"
              aria-pressed={value === "down"}
              className={cn(
                value === "down" &&
                  "bg-destructive/10 text-destructive hover:bg-destructive/20",
              )}
            >
              <ThumbsDown className="size-3.5" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>This wasn't right</TooltipContent>
        </Tooltip>
      </div>

      {showComment ? (
        <div className="flex items-center gap-1">
          <Input
            value={comment}
            onChange={(e) => setComment(e.target.value)}
            placeholder="What went wrong? (optional)"
            className="h-7 text-xs"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                submitComment();
              }
            }}
          />
          <Button
            variant="outline"
            size="sm"
            onClick={submitComment}
            className="h-7 text-xs"
          >
            Send
          </Button>
        </div>
      ) : null}
    </div>
  );
}
