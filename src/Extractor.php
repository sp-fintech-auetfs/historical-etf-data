<?php

namespace Hetfs;

use Carbon\Carbon;
use GuzzleHttp\Client;
use League\Flysystem\Filesystem;
use League\Flysystem\FilesystemException;
use League\Flysystem\Local\LocalFilesystemAdapter;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToCheckExistence;

class Extractor
{
    protected $today;

    protected $localContent;

    protected $remoteWebContent;

    protected $yticker;

    protected $yahooFinanceUrl = 'https://query1.finance.yahoo.com/v8/finance/chart/';

    protected $parsedCarbon = [];

    public function __construct()
    {
        //Increase Exectimeout to 1 hours as this process takes time to extract and merge data.
        if ((int) ini_get('max_execution_time') < 3600) {
            set_time_limit(3600);
        }

        $this->today = Carbon::today();

        $this->localContent = new Filesystem(
            new LocalFilesystemAdapter(
                __DIR__ . '/../',
                null,
                LOCK_EX,
                LocalFilesystemAdapter::SKIP_LINKS,
                null,
                false
            ),
            []
        );

        $this->remoteWebContent = new Client(
            [
                'debug'           => false,
                'http_errors'     => true,
                'timeout'         => 2,
                'verify'          => false
            ]
        );

        try {
            if ($this->localContent->fileExists('src/yticker.json')) {
                $this->yticker = json_decode($this->localContent->read('src/yticker.json'), true);
            }
        } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
            throw $e;
        }

        if (!$this->yticker) {
            throw new \Exception('yticker.json file not found or incorrect!');
        }
    }

    public function extract()
    {
        $this->parsedCarbon[$this->today->timestamp] = $this->today;

        $period1 = $this->today->copy()->subDay()->timestamp;
        $period2 = $this->today->timestamp;

        foreach ($this->yticker as $ticker) {
            $tickerFile = null;

            //Read current data json file
            try {
                if ($this->localContent->fileExists('data/' . $ticker . '.json')) {
                    $tickerFile = json_decode($this->localContent->read('data/' . $ticker . '.json'), true);
                }
            } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                throw $e;
            }

            if ($tickerFile) {
                if (isset($tickerFile['last_updated'])) {
                    if (!isset($this->parsedCarbon[$tickerFile['last_updated']])) {
                        $this->parsedCarbon[$tickerFile['last_updated']] = \Carbon\Carbon::parse($tickerFile['last_updated']);
                    }

                    if ($this->parsedCarbon[$tickerFile['last_updated']]->copy()->startOfDay()->timestamp === $period2) {
                        continue;
                    }

                    $period1 = $this->parsedCarbon[$tickerFile['last_updated']]->copy()->startOfDay()->timestamp;
                }

                if (isset($tickerFile['meta']['firstTradeDate']) && isset($tickerFile['quote']) && count($tickerFile['quote']) > 0) {
                    $firstQuote = array_shift($tickerFile['quote']);

                    if ($firstQuote['timestamp'] !== $tickerFile['meta']['firstTradeDate']) {
                        $period1 = $tickerFile['meta']['firstTradeDate'];
                    }
                }
            }

            if ($period1 === $period2) {
                continue;
            }

            if (!$ytickerData = $this->getYtickerData($ticker, $period1, $period2)) {
                continue;
            }

            if (isset($ytickerData['chart']['result'][0]['meta']) && !$tickerFile) {
                $period1 = $ytickerData['chart']['result'][0]['meta']['firstTradeDate'];

                if (!$ytickerData = $this->getYtickerData($ticker, $period1, $period2)) {
                    continue;
                }
            }

            $ytickerCombined = [];

            if (isset($ytickerData['chart']['result'][0]['meta'])) {
                $ytickerCombined['meta'] = $ytickerData['chart']['result'][0]['meta'];
            }

            if (isset($ytickerData['chart']['result'][0]['timestamp']) && isset($ytickerData['chart']['result'][0]['indicators']['quote'][0]['close'])) {
                $timestampKeyCount = count($ytickerData['chart']['result'][0]['timestamp']);
                $timestampKeyCounter = 0;

                foreach ($ytickerData['chart']['result'][0]['timestamp'] as $timestampKey => $timestamp) {
                    if (!isset($this->parsedCarbon[$timestamp])) {
                        $this->parsedCarbon[$timestamp] = \Carbon\Carbon::parse($timestamp);
                    }

                    if ($ytickerData['chart']['result'][0]['indicators']['quote'][0]['open'][$timestampKey]) {
                        $ytickerCombined['quote'][$timestamp]['timestamp'] = $timestamp;
                        $ytickerCombined['quote'][$timestamp]['date'] = $this->parsedCarbon[$timestamp]->toDateString();
                        $ytickerCombined['quote'][$timestamp]['open'] = $ytickerData['chart']['result'][0]['indicators']['quote'][0]['open'][$timestampKey];
                        $ytickerCombined['quote'][$timestamp]['close'] = $ytickerData['chart']['result'][0]['indicators']['quote'][0]['close'][$timestampKey];
                        $ytickerCombined['quote'][$timestamp]['adjclose'] = $ytickerData['chart']['result'][0]['indicators']['adjclose'][0]['adjclose'][$timestampKey];
                        $ytickerCombined['quote'][$timestamp]['low'] = $ytickerData['chart']['result'][0]['indicators']['quote'][0]['low'][$timestampKey];
                        $ytickerCombined['quote'][$timestamp]['high'] = $ytickerData['chart']['result'][0]['indicators']['quote'][0]['high'][$timestampKey];
                        $ytickerCombined['quote'][$timestamp]['volume'] = $ytickerData['chart']['result'][0]['indicators']['quote'][0]['volume'][$timestampKey];
                        if (isset($ytickerData['chart']['result'][0]['events']['dividends'][$timestamp]) &&
                            isset($ytickerData['chart']['result'][0]['events']['dividends'][$timestamp]['amount'])
                        ) {
                            $ytickerCombined['quote'][$timestamp]['dividends_amount'] = $ytickerData['chart']['result'][0]['events']['dividends'][$timestamp]['amount'];
                        }
                    }

                    $timestampKeyCounter++;

                    if ($timestampKeyCounter === $timestampKeyCount) {//Last Entry
                        $ytickerCombined['last_updated'] = $timestamp;

                        try {
                            $this->localContent->write('data/' . strtoupper($ticker) . '.json', json_encode($ytickerCombined));
                        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                            throw $e;
                        }

                        sleep(1);
                    }
                }
            }
        }
    }

    protected function getYtickerData($ticker, $period1, $period2)
    {
        try {
            $auTicker = strtoupper($ticker) . '.AX';

            $tickerUrl = $this->yahooFinanceUrl . $auTicker . '?events=capitalGain|div|split&interval=1d&lang=en-AU&region=AU&period1=';
            $tickerUrl .= $period1 . '&period2=' . $period2;

            $response = $this->remoteWebContent->get($tickerUrl);

            if ($response->getStatusCode() === 200) {
                return json_decode($response->getBody()->getContents(), true);
            }
        } catch (\throwable $e) {
            if (str_contains($e->getMessage(), 'delisted')) {
                return false;
            }

            throw new \Exception('yticker for ' . $ticker . ' has incorrect data');
        }
    }
}