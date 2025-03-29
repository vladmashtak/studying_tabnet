package main

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var zipURLs = []string{

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2025-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2025-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2024-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2023-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2022-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2020-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2019-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-08.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-07.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-06.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-05.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-04.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-03.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-02.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2018-01.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2017-12.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2017-11.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2017-10.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2017-09.zip",

	"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2017-08.zip",
}

func main() {
	var allRecords [][]string

	tempDir := "archive_btc_binance_spot"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		panic(err)
	}

	for i, url := range zipURLs {
		fmt.Printf("Скачиваем файл %s\n", url)
		filePath := strings.Split(url, "/")
		fileName := "file_%d.zip"
		if len(filePath) > 0 {
			fileName = filePath[len(filePath)-1]
		}
		zipPath := fmt.Sprintf(fileName, i)

		if err := downloadFile(zipPath, url); err != nil {
			panic(fmt.Sprintf("Не удалось скачать файл %s: %v", url, err))
		}

		csvPath, err := unzipCSV(zipPath, tempDir)
		if err != nil {
			panic(fmt.Sprintf("Ошибка распаковки %s: %v", zipPath, err))
		}

		records, err := readCSV(csvPath)
		if err != nil {
			panic(fmt.Sprintf("Ошибка чтения CSV %s: %v", csvPath, err))
		}

		if i == 0 {
			allRecords = records
		} else {
			if len(records) > 1 {
				allRecords = append(allRecords, records[1:]...)
			}
		}
	}

	outCSV := "dataset.csv"
	if err := writeCSV(outCSV, allRecords); err != nil {
		panic(fmt.Sprintf("Ошибка записи итогового CSV: %v", err))
	}

	if err := normalizeCSV(outCSV); err != nil {
		panic(fmt.Sprintf("Ошибка нормализации CSV: %v", err))
	}

	fmt.Printf("датасет сохранён в %s\n", outCSV)
}

func downloadFile(filePath, url string) error {
	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("не удалось скачать файл, статус: %s", resp.Status)
	}

	_, err = io.Copy(out, resp.Body)
	return err
}

func unzipCSV(zipPath, destDir string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", err
	}
	defer r.Close()

	var csvFilePath string

	for _, f := range r.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			extractedPath := filepath.Join(destDir, f.Name)
			if err := os.MkdirAll(filepath.Dir(extractedPath), 0755); err != nil {
				return "", err
			}

			rc, err := f.Open()
			if err != nil {
				return "", err
			}
			defer rc.Close()

			outFile, err := os.Create(extractedPath)
			if err != nil {
				return "", err
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, rc); err != nil {
				return "", err
			}

			csvFilePath = extractedPath
			break
		}
	}

	if csvFilePath == "" {
		return "", fmt.Errorf("в архиве %s не найден файл *.csv", zipPath)
	}

	return csvFilePath, nil
}

func readCSV(filePath string) ([][]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	var records [][]string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func writeCSV(filePath string, data [][]string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	for _, record := range data {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func normalizeCSV(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	const openingTimeIndex = 0
	const closingTimeIndex = 6

	for i, record := range records {
		openTime := record[openingTimeIndex]
		closeTime := record[closingTimeIndex]

		if len(openTime) == 16 {
			records[i][openingTimeIndex] = openTime[0:13]
		}

		if len(closeTime) == 16 {
			records[i][closingTimeIndex] = closeTime[0:13]
		}
	}

	out, err := os.Create("dataset_normalized.csv")
	if err != nil {
		return err
	}

	defer out.Close()

	writer := csv.NewWriter(out)
	defer writer.Flush()
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
