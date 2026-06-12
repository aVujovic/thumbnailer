import { describe, it, expect } from 'vitest';
import type { FileInfo } from '@thumbnailer/domain';
import { VideoDetector } from './videoDetector.js';

const detector = new VideoDetector({
  config: { videoExtensions: ['mp4', 'mov', 'mkv'] },
});

function file(over: Partial<FileInfo>): FileInfo {
  return { name: 'clip.mp4', isDirectory: false, size: 100, permissions: 0o644, ...over };
}

describe('VideoDetector', () => {
  it('accepts a readable, non-empty file with a known extension', () => {
    expect(detector.detect(file({ name: 'movie.mp4' }))).toEqual({ video: true });
  });

  it('is case-insensitive on the extension', () => {
    expect(detector.isVideo(file({ name: 'CLIP.MOV' }))).toBe(true);
    expect(detector.isVideo(file({ name: 'a.MkV' }))).toBe(true);
  });

  it('rejects directories', () => {
    expect(detector.detect(file({ isDirectory: true, name: 'movies' }))).toEqual({
      video: false,
      reason: 'directory',
    });
  });

  it('rejects unknown / missing extensions', () => {
    expect(detector.detect(file({ name: 'notes.txt' })).video).toBe(false);
    expect(detector.detect(file({ name: 'README' })).video).toBe(false);
    expect(detector.detect(file({ name: '.mp4' })).video).toBe(false); // dotfile, no base name
    expect(detector.detect(file({ name: 'trailing.' })).video).toBe(false);
  });

  it('rejects empty (size 0) files before ffmpeg', () => {
    expect(detector.detect(file({ size: 0 }))).toEqual({ video: false, reason: 'empty' });
  });

  it('rejects files without the owner-read bit', () => {
    expect(detector.detect(file({ permissions: 0o044 }))).toEqual({
      video: false,
      reason: 'unreadable',
    });
  });
});
