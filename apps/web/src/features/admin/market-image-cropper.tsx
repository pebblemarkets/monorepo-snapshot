import React from "react";
import Cropper, { type Area } from "react-easy-crop";
import "react-easy-crop/react-easy-crop.css";

import { AspectRatio } from "@/components/ui/aspect-ratio";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Slider } from "@/components/ui/slider";
import { cn } from "@/lib/utils";

type MarketImageCropperProps = {
  label: string;
  hint: string;
  file: File | null;
  outputWidth: number;
  outputHeight: number;
  aspectRatio: number;
  onChange: (file: File | null) => void;
  disabled?: boolean;
};

const WEBP_EXPORT_QUALITY = 0.72;
type CropperPoint = {
  x: number;
  y: number;
};

type CropperComponentProps = {
  image: string;
  crop: CropperPoint;
  zoom: number;
  aspect: number;
  showGrid?: boolean;
  objectFit?: "contain" | "cover" | "horizontal-cover" | "vertical-cover";
  onCropChange: (location: CropperPoint) => void;
  onZoomChange: (zoom: number) => void;
  onCropComplete: (area: Area, areaPixels: Area) => void;
};

const ReactEasyCrop = Cropper as unknown as React.ComponentType<CropperComponentProps>;

function formatDimensionLabel(width: number, height: number): string {
  return `${width}x${height}`;
}

function fileBaseName(fileName: string): string {
  const trimmed = fileName.trim();
  if (!trimmed) {
    return "asset";
  }
  const withoutExt = trimmed.replace(/\.[^./\\]+$/, "");
  const normalized = withoutExt
    .replace(/[^a-zA-Z0-9-_]+/g, "-")
    .replace(/-+/g, "-")
    .toLowerCase()
    .slice(0, 48);
  return normalized || "asset";
}

function loadImage(sourceUrl: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("failed to load source image"));
    image.src = sourceUrl;
  });
}

async function createCroppedWebpFile(
  sourceUrl: string,
  cropArea: Area,
  outputWidth: number,
  outputHeight: number,
  fileNamePrefix: string,
): Promise<File> {
  const sourceImage = await loadImage(sourceUrl);
  const canvas = document.createElement("canvas");
  canvas.width = outputWidth;
  canvas.height = outputHeight;

  const context = canvas.getContext("2d");
  if (!context) {
    throw new Error("canvas context is unavailable");
  }

  const sourceX = Math.max(0, Math.round(cropArea.x));
  const sourceY = Math.max(0, Math.round(cropArea.y));
  const sourceWidth = Math.max(1, Math.round(cropArea.width));
  const sourceHeight = Math.max(1, Math.round(cropArea.height));

  const boundedWidth = Math.min(sourceWidth, sourceImage.naturalWidth - sourceX);
  const boundedHeight = Math.min(sourceHeight, sourceImage.naturalHeight - sourceY);
  if (boundedWidth <= 0 || boundedHeight <= 0) {
    throw new Error("invalid crop area");
  }

  context.clearRect(0, 0, outputWidth, outputHeight);
  context.imageSmoothingEnabled = true;
  context.imageSmoothingQuality = "high";
  context.drawImage(
    sourceImage,
    sourceX,
    sourceY,
    boundedWidth,
    boundedHeight,
    0,
    0,
    outputWidth,
    outputHeight,
  );

  const blob = await new Promise<Blob | null>((resolve) => {
    canvas.toBlob(resolve, "image/webp", WEBP_EXPORT_QUALITY);
  });
  if (!blob) {
    throw new Error("failed to encode cropped image");
  }

  return new File([blob], `${fileNamePrefix}.webp`, { type: "image/webp" });
}

export function MarketImageCropper(props: MarketImageCropperProps) {
  const {
    label,
    hint,
    file,
    outputWidth,
    outputHeight,
    aspectRatio,
    onChange,
    disabled = false,
  } = props;

  const [sourceUrl, setSourceUrl] = React.useState<string | null>(null);
  const [sourceFileName, setSourceFileName] = React.useState<string>("asset");
  const [dialogOpen, setDialogOpen] = React.useState<boolean>(false);
  const [crop, setCrop] = React.useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [zoom, setZoom] = React.useState<number>(1);
  const [cropAreaPixels, setCropAreaPixels] = React.useState<Area | null>(null);
  const [pending, setPending] = React.useState<boolean>(false);
  const [fieldError, setFieldError] = React.useState<string>("");

  const previewUrl = React.useMemo(() => {
    if (!file) {
      return null;
    }
    return URL.createObjectURL(file);
  }, [file]);

  React.useEffect(() => {
    return () => {
      if (previewUrl) {
        URL.revokeObjectURL(previewUrl);
      }
    };
  }, [previewUrl]);

  React.useEffect(() => {
    return () => {
      if (sourceUrl) {
        URL.revokeObjectURL(sourceUrl);
      }
    };
  }, [sourceUrl]);

  const closeCropDialog = React.useCallback(() => {
    setDialogOpen(false);
    setCrop({ x: 0, y: 0 });
    setZoom(1);
    setCropAreaPixels(null);
    setFieldError("");
    setPending(false);
    setSourceUrl((current) => {
      if (current) {
        URL.revokeObjectURL(current);
      }
      return null;
    });
  }, []);

  const openCropDialogForFile = React.useCallback(
    (nextFile: File) => {
      setSourceUrl((current) => {
        if (current) {
          URL.revokeObjectURL(current);
        }
        return URL.createObjectURL(nextFile);
      });
      setSourceFileName(fileBaseName(nextFile.name));
      setCrop({ x: 0, y: 0 });
      setZoom(1);
      setCropAreaPixels(null);
      setFieldError("");
      setDialogOpen(true);
    },
    [],
  );

  const onFileInputChange = React.useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const pickedFile = event.target.files?.[0] ?? null;
      if (pickedFile) {
        openCropDialogForFile(pickedFile);
      }
      event.target.value = "";
    },
    [openCropDialogForFile],
  );

  const applyCrop = React.useCallback(async () => {
    if (!sourceUrl || !cropAreaPixels) {
      setFieldError("Pick an image and adjust the crop first.");
      return;
    }

    setPending(true);
    setFieldError("");
    try {
      const croppedFile = await createCroppedWebpFile(
        sourceUrl,
        cropAreaPixels,
        outputWidth,
        outputHeight,
        sourceFileName,
      );
      onChange(croppedFile);
      closeCropDialog();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "failed to crop image";
      setFieldError(message);
      setPending(false);
    }
  }, [
    closeCropDialog,
    cropAreaPixels,
    onChange,
    outputHeight,
    outputWidth,
    sourceFileName,
    sourceUrl,
  ]);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="text-[11px] text-muted-foreground">
          {formatDimensionLabel(outputWidth, outputHeight)}
        </div>
      </div>

      <AspectRatio ratio={aspectRatio} className="overflow-hidden border bg-muted/40">
        {previewUrl ? (
          <img
            src={previewUrl}
            alt=""
            className="absolute inset-0 h-full w-full object-cover"
            loading="lazy"
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center px-3 text-center text-xs text-muted-foreground">
            {hint}
          </div>
        )}
      </AspectRatio>

      <div className="flex flex-wrap items-center gap-2">
        <Input
          type="file"
          accept="image/png,image/jpeg,image/webp,image/gif,image/avif"
          onChange={onFileInputChange}
          disabled={disabled}
          className="max-w-full"
        />
        {file ? (
          <Button
            type="button"
            variant="outline"
            size="xs"
            onClick={() => onChange(null)}
            disabled={disabled}
          >
            Clear
          </Button>
        ) : null}
      </div>

      <Dialog
        open={dialogOpen}
        onOpenChange={(open) => {
          if (!open) {
            closeCropDialog();
          }
        }}
      >
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>{label}</DialogTitle>
            <DialogDescription>
              Crop to {formatDimensionLabel(outputWidth, outputHeight)} and export as WebP.
            </DialogDescription>
          </DialogHeader>

          <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_260px]">
            <div className="relative h-[340px] overflow-hidden border bg-muted">
              {sourceUrl ? (
                <ReactEasyCrop
                  image={sourceUrl}
                  crop={crop}
                  zoom={zoom}
                  aspect={aspectRatio}
                  showGrid
                  objectFit="contain"
                  onCropChange={setCrop}
                  onZoomChange={setZoom}
                  onCropComplete={(_area: Area, areaPixels: Area) => setCropAreaPixels(areaPixels)}
                />
              ) : null}
            </div>

            <div className="space-y-3">
              <div className="space-y-1">
                <div className="text-xs text-muted-foreground">Zoom</div>
                <Slider
                  min={1}
                  max={3}
                  step={0.01}
                  value={[zoom]}
                  onValueChange={(values) => {
                    const next = Array.isArray(values) ? values[0] : values;
                    if (typeof next === "number") {
                      setZoom(next);
                    }
                  }}
                />
              </div>

              <AspectRatio ratio={aspectRatio} className="overflow-hidden border bg-muted/40">
                {sourceUrl ? (
                  <div
                    className={cn(
                      "absolute inset-0 bg-cover bg-center bg-no-repeat",
                      pending ? "opacity-70" : "opacity-100",
                    )}
                    style={{ backgroundImage: `url("${sourceUrl}")` }}
                  />
                ) : null}
              </AspectRatio>

              {fieldError ? <div className="text-xs text-destructive">{fieldError}</div> : null}
            </div>
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={closeCropDialog} disabled={pending}>
              Cancel
            </Button>
            <Button type="button" onClick={() => void applyCrop()} disabled={pending}>
              {pending ? "Applying..." : "Apply crop"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
