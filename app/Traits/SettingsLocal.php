<?php

namespace App\Traits;

trait SettingsLocal {

    private ?string $inputPath = null;
    private ?string $outputPath = null;

    // Tuning configurations
    const int WORKER_COUNT      = 28;                    // Should match physical core count
    const int PRESCAN_BUFFER    = 256 * 1024;           // 256kb - just enough to see all 268 urls
    const int READ_BUFFER       = 16 * 1024 * 1024;     // 64mb - Bumping up since we have 12gb of memory available
    const int WRITE_BUFFER      = 128 * 1024;

    const int SHM_MAX_SEGMENTS = 16;
    const int SHM_MAX_SEGMENT_SIZE = 32 * 1024 * 1024;

    const int CALIBRATION_DUR               = 50;
    const float CALIBRATION_PARENT_FACTOR   = 0.95;

    // Token Tables
    private array $urlPool              = [];   // url_string -> true
    private array $urlTokens            = [];   // url_string -> int token
    private array $urlTokensShifted     = [];   // url_string -> int token * DATE_COUNT
    private array $urlStrings           = [];   // int token -> url_string
    private int $urlCount               = 268;

    private array $dateChars            = [];   // date_string -> 2 byte packed cha
    private array $dateStrings          = [];   // int token -> date_string "2026-01-01"
    private array $dateTokens           = [];   // date_string -> int token
    private int $dateCount              = 2008;

}