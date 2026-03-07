<?php

declare(strict_types=1);

namespace App;

use App\Traits\LoaderHybridV1Trait;
use App\Traits\LoaderTokenizedFileV1Trait;
use App\Traits\LoaderTokenizedSocketV2Trait;
use App\Traits\WorkerHybridV1Trait;
use App\Traits\WorkerTokenizedFileV1Trait;
use App\Traits\WorkerTokenizedShmopV1Trait;
use App\Traits\WorkerTokenizedSocketV1Trait;
use App\Traits\WorkerTokenizedSocketV2Trait;
use App\Traits\WriterTokenizedV1Trait;
use App\Traits\SetupTokenizedV1Trait;
use App\Traits\LoaderTokenizedSocketV1Trait;
use App\Traits\LoaderTokenizedSocketV1iTrait;
use App\Traits\LoaderTokenizedShmopV1Trait;

final class Parser
{
    private ?string $inputPath = null;
    private ?string $outputPath = null;

    // Schema parsing configurations
    const int DOMAIN_LENGTH     = 25;   // prefix chars to strip from URL field
                                        // "https://stitcher.io/blog/"
    const int DATE_LENGTH       = 10;   // "2026-01-01"
    const int DATE_WIDTH        = 25;   // Full datetime column width inc. time component

    // Tuning configurations
    const int WORKER_COUNT      = 22;                    // Should match physical core count
    const int CALIBRATION_DUR   = 100;                   // 50ms overhead time for calibrating chunk boundaries
    const int WRITE_BUFFER      = 128 * 1024;           // 128kb output write buffer
    const int PRESCAN_BUFFER    = 256 * 1024;           // 256kb - enough to see all 269 urls
    const int READ_BUFFER       = 8 * 1024 * 1024;     // 8mb

    // Token Tables
    private array $urlPool              = [];   // url_string -> true
    private array $urlTokens            = [];   // url_string -> int token
    private array $dateTokens           = [];   // packed_int -> int token
    private array $dateStrTokens        = [];   // date_string -> int token
    private array $dateChars            = [];   // date_string -> 2 byte packed char
    private array $urlStrings           = [];   // int token -> url_string
    private array $dateStrings          = [];   // int token -> date_string "2026-01-01"
    private array $urlJsonKeys          = [];
    private array $dateJsonPrefixes     = [];
    private int $urlCount               = 0;
    private int $dateCount              = 0;
    private int $minUrlLength           = 999;
    private int $minLineLength          = 35;

    // Tokenized Socket Implementation
    use SetupTokenizedV1Trait;
    use LoaderTokenizedSocketV2Trait;
    use WorkerTokenizedSocketV2Trait;
    use WriterTokenizedV1Trait;

    // Tokenized File Implementation
//    use SetupTokenizedV1Trait;
//    use LoaderTokenizedFileV1Trait;
//    use WorkerTokenizedFileV1Trait;
//    use WriterTokenizedV1Trait;


    // Tokenized Hybrid Implementation
//    use SetupTokenizedV1Trait;
//    use LoaderHybridV1Trait;
//    use WorkerHybridV1Trait;
//    use WriterTokenizedV1Trait;

    // Meme
//    use SetupTokenizedV1Trait;
//    use MemeTrait;
//    use WriterTokenizedV1Trait;

    /**
     * This is the main data structure that is populated by the load
     * process and then written to the output file by the writer
     */
    private array $data = [];

    public function __construct()
    {
        // Eliminate GC pauses mid-parse
        gc_disable();

        // Disable library error handler from killing the process on intentionally suppressed errors
        // set_error_handler(null);
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->inputPath = $inputPath;
        $this->outputPath = $outputPath;

        // -- Phase 0: Setup ----
        $t0 = hrtime(true);
        $this->setup();
        printf("Setup took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 1: Load the data ----
        $t0 = hrtime(true);
        $this->data = $this->load();
        printf("Load  took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 3: Write Output ----
        $t0 = hrtime(true);
        $this->write();
        printf("Write took %.2fms\n", (hrtime(true) - $t0) / 1e6);
    }

}