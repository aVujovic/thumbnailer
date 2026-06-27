-- CreateEnum
CREATE TYPE "VideoStatus" AS ENUM ('generated', 'skipped', 'failed');

-- CreateTable
CREATE TABLE "videos" (
    "video_path" TEXT NOT NULL,
    "scan_id" TEXT,
    "tenant_id" TEXT,
    "output_path" TEXT NOT NULL,
    "size" BIGINT NOT NULL,
    "discovered_at" BIGINT NOT NULL,
    "format" TEXT NOT NULL,
    "status" "VideoStatus" NOT NULL,
    "attempts" INTEGER NOT NULL,
    "error" TEXT,
    "processed_at" BIGINT NOT NULL,
    "synced" BOOLEAN NOT NULL DEFAULT false,
    "synced_at" BIGINT,

    CONSTRAINT "videos_pkey" PRIMARY KEY ("video_path")
);

-- CreateIndex
CREATE INDEX "videos_synced_idx" ON "videos"("synced");

-- CreateIndex
CREATE INDEX "videos_tenant_id_idx" ON "videos"("tenant_id");
