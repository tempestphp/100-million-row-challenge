<?php

namespace App\Traits;

trait SettingsMacMini {

    private ?string $inputPath = null;
    private ?string $outputPath = null;

    // Tuning configurations
    const int WORKER_COUNT      = 8;                    // Should match physical core count
    const int PRESCAN_BUFFER    = 256 * 1024;           // 256kb - just enough to see all 268 urls
    const int READ_BUFFER       = 64 * 1024 * 1024;     // 64mb - Bumping up since we have 12gb of memory available
    const int WRITE_BUFFER      = 128 * 1024;

    const int SHM_MAX_SEGMENTS = 8;
    const int SHM_MAX_SEGMENT_SIZE = 4 * 1024 * 1024;

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