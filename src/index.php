<?php

include 'vendor/autoload.php';

$extractor = new \Hetfs\Extractor;

try {
    $extractor->extract();
} catch (\throwable $e) {
    echo $e->getMessage();
}