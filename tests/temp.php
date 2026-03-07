<?php

namespace Tests;

use App\Traits\LoaderLegacyTraitV2;
use App\Traits\SettingsLocal;
use App\Traits\SettingsMacMini;
use App\Traits\SetupSerializedTrait;
use App\Traits\WorkerLegacyTraitV2;
use App\Traits\WorkerLegacyTraitV3;
use App\Traits\WriterTokenizedV1Trait;

/**
 * Just some tests, nothing to see here
 */
class Temp {

    use SettingsMacMini;
    //use SettingsLocal;
    use SetupSerializedTrait;
    use LoaderLegacyTraitV2;
    use WorkerLegacyTraitV3;
    use WriterTokenizedV1Trait;

    public function __construct()
    {
        gc_disable();
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $t0 = hrtime(true);

        $this->inputPath = $inputPath;
        $this->outputPath = $outputPath;

        // -- Phase 0: Setup ----
        //$t0 = hrtime(true);
        $this->setup();
        //printf("Setup took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 1: Load the data ----
        //$t0 = hrtime(true);
        $data = $this->load();
        //printf("Load  took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 3: Write Output ----
        //$t0 = hrtime(true);
        $this->write($data);
        //printf("Write took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        printf("Total took %.2fms\n", (hrtime(true) - $t0) / 1e6);
    }

}