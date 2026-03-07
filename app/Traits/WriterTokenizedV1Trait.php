<?php

namespace App\Traits;

trait WriterTokenizedV1Trait {

    /**
     * This writer version is currently running in around 165ms
     */
    private function write(array $data): void
    {
        // Hoist any class variables
        $accumulator = $data;
        $outputPath = $this->outputPath;

        // Build json key caches
        $urlJson = [];
        for ($i = 0; $i < $this->urlCount; $i++) {
            $urlJson[$i] = '    "\\/blog\\/' . str_replace('/', '\/', $this->urlStrings[$i] ?? '') . "\": {\n";
        }

        $dateJson = [];
        for ($i = 0; $i < $this->dateCount; $i++) {
            $dateJson[$i] = '        "' . $this->dateStrings[$i] . '": ';
        }


        $lastActiveUrlToken = -1;
        for ($urlToken = $this->urlCount - 1; $urlToken >= 0; $urlToken--) {
            $base = $urlToken * $this->dateCount;
            for ($d = 0; $d < $this->dateCount; $d++) {
                if ($accumulator[$base + $d] !== 0) {
                    $lastActiveUrlToken = $urlToken;
                    break 2;
                }
            }
        }

        $out = fopen($outputPath, 'wb', false);
        stream_set_write_buffer($out, 0);

        $buf = "{\n";

        for ($urlToken = 0; $urlToken <= $this->urlCount - 1; $urlToken++) {
            $base = $urlToken * $this->dateCount;

            $activeDateCount = 0;
            for ($d = 0; $d < $this->dateCount; $d++) {
                if ($accumulator[$base + $d] !== 0) $activeDateCount++;
            }

            if ($activeDateCount === 0) continue;

            $isLastUrl = ($urlToken === $lastActiveUrlToken);

            $dateBuf = '';
            for ($d = 0; $d < $this->dateCount; $d++) {
                $count = $accumulator[$base + $d];
                if ($count === 0) continue;
                $dateBuf .= $dateJson[$d] . $count . ",\n";
            }
            if ($dateBuf === '') continue;

            $buf .= $urlJson[$urlToken]
                . substr($dateBuf, 0, -2)  . "\n"
                . ($isLastUrl ? "    }\n" : "    },\n");

            if (strlen($buf) >= self::WRITE_BUFFER) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        fwrite($out, $buf . '}');
        fclose($out);
    }

}