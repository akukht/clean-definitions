package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type products struct {
	productID string `json:"product_id"`
}
type integrators struct {
	id string `json:"id"`
}

func main() {
	vendorsID := []string{
		"5cd9b730-1769-11eb-bd63-e60278779701",
		"127a2aa2-779d-11ec-b976-0242ac110007",
		"d962eff7-2786-11ec-b58f-0242ac110005",
		"30cb0497-566a-11ec-ba8d-0242ac11000c",
		"77eb6639-19e4-11eb-849a-0242ac110004",
		"79ab2735-ab7a-11ec-b72e-0242ac110007",
	}

	allProdIDs, err := getProductsByVendorIDs(vendorsID)
	if err != nil {
		log.Fatal(err)
	}

	allIntegrators, err := getIntegratorIDs(allProdIDs)
	if err != nil {
		log.Fatal(err)
	}

	err = writeIDToFile(allIntegrators)
	if err != nil {
		log.Fatal(err)
	}
}

func getProductsByVendorIDs(vendorIDs []string) ([]products, error) {
	var prodIDs []products
	var productMap []map[string]interface{}

	for _, vendorID := range vendorIDs {
		url := "http://:8890/openapi/v1/mgmt/vendors/" + vendorID + "/products"
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Accept", "*/*")
		req.Header.Add("iPlanetDirectoryPro", "AQIC5wM2LY4SfcxtULBDq9jaRIBheeaemzvq3YAchZV1EP8.*AAJTSQACMDIAAlNLABQtMjEzOTUyNjQ2MTU4MDM4NTkzMwACUzEAAjAx*")
		req.Header.Add("uid", "dtuser@dtuser.com")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		err = json.Unmarshal(body, &productMap)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		for _, data := range productMap {
			var p products
			p.productID = fmt.Sprintf("%s", data["product_id"])
			prodIDs = append(prodIDs, p)
		}
		res.Body.Close()
	}

	return prodIDs, nil
}

func writeIDToFile(productID []integrators) error {
	file, err := os.Create("definitions.txt")
	if err != nil {
		log.Fatal(err)
		return err
	}
	for _, item := range productID {
		fmt.Println(item.id)
		_, err = io.WriteString(file, item.id+"\n")
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

func getIntegratorIDs(integratorIDs []products) ([]integrators, error) {
	var integIDs []integrators
	var integratorMap []map[string]interface{}

	for _, integratorID := range integratorIDs {
		url := "http://:8080/extended-attributes/v1/integrators/" + integratorID.productID + "/definitions"
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		req.Header.Add("Accept", "*/*")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		err = json.Unmarshal(body, &integratorMap)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		for _, data := range integratorMap {
			var p integrators
			p.id = fmt.Sprintf("%s", data["id"])
			integIDs = append(integIDs, p)
		}
		res.Body.Close()
	}

	return integIDs, nil
}

