<?php

namespace App;

use function array_fill;
use function chr;
use function chunk_split;
use function count;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function sodium_add;
use function str_repeat;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $mapaFechas = [];
        $listaFechas = [];
        $totalFechas = 0;

        $y = 1;
        while ($y <= 6) {
            $m = 1;
            while ($m <= 12) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";

                $d = 1;
                while ($d <= $maxD) {
                    $clave = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $mapaFechas[$clave] = $totalFechas;
                    $listaFechas[$totalFechas] = '202' . $clave;
                    $totalFechas++;
                    $d++;
                }
                $m++;
            }
            $y++;
        }

        $incChar = [];
        $i = 0;
        while ($i < 255) {
            $incChar[chr($i)] = chr($i + 1);
            $i++;
        }

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $crudo = fread($fh, 181_000);

        $prefijoBase = 'https://stitcher.io/blog/';
        $rutas = [];
        $mapaRutas = [];
        $cantidadRutas = 0;
        $puntero = 0;
        $ultimoSalto = strrpos($crudo, "\n") ?: 0;

        while ($puntero < $ultimoSalto && $cantidadRutas < 268) {
            $nl = strpos($crudo, "\n", $puntero + 52);
            if ($nl === false) break;

            $slug = substr($crudo, $puntero + 25, $nl - $puntero - 51);
            if (!isset($mapaRutas[$slug])) {
                $rutas[$cantidadRutas] = $slug;
                $mapaRutas[$slug] = $cantidadRutas * $totalFechas;
                $cantidadRutas++;
            }
            $puntero = $nl + 1;
        }

        $largoCola = 22;
        $desplazamiento = 20;
        $mascara = (1 << $desplazamiento) - 1;
        $zancadaMax = 100;
        $mapaRutas = [];

        $p = 0;
        while ($p < $cantidadRutas) {
            $zancada = strlen($rutas[$p]) + 52;
            $mapaRutas[substr($prefijoBase . $rutas[$p], -$largoCola)] = ($zancada << $desplazamiento) | ($p * $totalFechas);
            $p++;
        }

        $offsetCola = 26 + $largoCola;
        $offsetFecha = 22;
        $limiteSeguridad = ($zancadaMax * 10) + $offsetCola;
        $tamanoSalida = $cantidadRutas * $totalFechas;

        fseek($fh, 0, SEEK_END);
        $tamanoArchivo = ftell($fh);

        $grano = 1 << 24;
        $segmentos = [];
        $bajo = 0;

        while ($bajo < $tamanoArchivo) {
            $alto = $bajo + $grano;
            if ($alto > $tamanoArchivo) $alto = $tamanoArchivo;
            $desde = 0;
            if ($bajo > 0) {
                fseek($fh, $bajo);
                fgets($fh);
                $desde = ftell($fh);
            }
            $hasta = $tamanoArchivo;
            if ($alto < $tamanoArchivo) {
                fseek($fh, $alto);
                fgets($fh);
                $hasta = ftell($fh);
            }
            $segmentos[] = [$desde, $hasta];
            $bajo = $alto;
        }
        fclose($fh);
        $totalSegmentos = count($segmentos);

        $trabajadores = 9;
        $sockets = [];
        $w = 0;

        while ($w < $trabajadores) {
            $par = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($par[0], $tamanoSalida * 2);
            stream_set_chunk_size($par[1], $tamanoSalida * 2);

            if (pcntl_fork() === 0) {
                $salida = str_repeat("\0", $tamanoSalida);
                $lector = fopen($inputPath, 'rb');
                stream_set_read_buffer($lector, 0);

                $ci = $w;
                while ($ci < $totalSegmentos) {
                    [$desde, $hasta] = $segmentos[$ci];
                    fseek($lector, $desde);
                    $restante = $hasta - $desde;

                    while ($restante > 0) {
                        $pedazo = fread($lector, $restante > 131_072 ? 131_072 : $restante);
                        $largoPedazo = strlen($pedazo);
                        $restante -= $largoPedazo;

                        $ultimoNl = strrpos($pedazo, "\n");
                        if ($ultimoNl === false) break;

                        $colaFinal = $largoPedazo - $ultimoNl - 1;
                        if ($colaFinal > 0) {
                            fseek($lector, -$colaFinal, SEEK_CUR);
                            $restante += $colaFinal;
                        }

                        $ptr = $ultimoNl;

                        while ($ptr > $limiteSeguridad) {
                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;

                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;
                        }

                        while ($ptr >= $offsetCola) {
                            $empaquetado = $mapaRutas[substr($pedazo, $ptr - $offsetCola, $largoCola)];
                            $indice = ($empaquetado & $mascara) + $mapaFechas[substr($pedazo, $ptr - $offsetFecha, 7)];
                            $salida[$indice] = $incChar[$salida[$indice]];
                            $ptr -= $empaquetado >> $desplazamiento;
                        }
                    }
                    $ci += $trabajadores;
                }

                fclose($lector);
                fwrite($par[1], chunk_split($salida, 1, "\0"));
                exit(0);
            }
            fclose($par[1]);
            $sockets[$w] = $par[0];
            $w++;
        }

        $buffers = array_fill(0, $trabajadores, '');
        $escritura = [];
        $excepcion = [];

        while ($sockets !== []) {
            $lectura = $sockets;
            stream_select($lectura, $escritura, $excepcion, 5);
            foreach ($lectura as $llave => $soc) {
                $datos = fread($soc, $tamanoSalida * 2);
                if ($datos !== '' && $datos !== false) {
                    $buffers[$llave] .= $datos;
                }
                if (feof($soc)) {
                    fclose($soc);
                    unset($sockets[$llave]);
                }
            }
        }

        $mezclaFinal = $buffers[0];
        $iteradorMerge = $trabajadores - 1;
        while ($iteradorMerge > 0) {
            sodium_add($mezclaFinal, $buffers[$iteradorMerge]);
            $iteradorMerge--;
        }
        $conteos = unpack('v*', $mezclaFinal);

        $salidaArch = fopen($outputPath, 'wb');
        stream_set_write_buffer($salidaArch, 2_097_152);
        fwrite($salidaArch, '{');

        $prefijosFecha = [];
        $d = 0;
        while ($d < $totalFechas) {
            $prefijosFecha[$d] = "        \"{$listaFechas[$d]}\": ";
            $d++;
        }

        $rutasEscapadas = [];
        $p = 0;
        while ($p < $cantidadRutas) {
            $rutasEscapadas[$p] = '"\/blog\/' . $rutas[$p] . '": {';
            $p++;
        }

        $separador = "\n    ";
        $base = 1;

        $p = 0;
        while ($p < $cantidadRutas) {
            $primeraFecha = -1;
            $idx = $base;

            $d = 0;
            while ($d < $totalFechas) {
                if ($conteos[$idx] !== 0) { $primeraFecha = $d; break; }
                $idx++;
                $d++;
            }

            if ($primeraFecha === -1) {
                $base += $totalFechas;
                $p++;
                continue;
            }

            $bloque = $separador . $rutasEscapadas[$p] . "\n" . $prefijosFecha[$primeraFecha] . $conteos[$idx];
            $separador = ",\n    ";

            $d = $primeraFecha + 1;
            while ($d < $totalFechas) {
                $idx++;
                $conteo = $conteos[$idx];
                if ($conteo !== 0) {
                    $bloque .= ",\n" . $prefijosFecha[$d] . $conteo;
                }
                $d++;
            }

            $bloque .= "\n    }";
            fwrite($salidaArch, $bloque);
            $base += $totalFechas;
            $p++;
        }

        fwrite($salidaArch, "\n}");
        fclose($salidaArch);
    }
}
