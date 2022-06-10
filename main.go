package main

import (
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	conn, err := SetupStorage()
	if err != nil {
		log.Println("Connection to database failed:", err)
	}
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
		log.Fatal("", err)
	}

	allIntegrators, err := conn.getDefinitionsFromDB(allProdIDs)
	if err != nil {
		log.Fatal(err)
	}

	err = writeIDToFile(allIntegrators)
	if err != nil {
		log.Fatal("an error occurred while writing the ID:", err)
	}

	err = conn.clearData()
	if err != nil {
		log.Fatal("truncate error:", err)
	}
	err = conn.AddDefinition(allIntegrators)
	if err != nil {
		fmt.Println("AddDefinition Error:", err)
	}

}

func (s *Storage) AddDefinition(allIntegrators []VendorAttributeDefinitions) error {
	for _, definition := range allIntegrators {
		if err := s.db.Query(`INSERT INTO vendor_attribute_definitions 
		(integrator_id, entity_type, attribute_id, active, created_at, created_by, default_value, description, entitlement_feature_name, is_hidden, localizations, name, type, ui_options, updated_at, updated_by, validation_options) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			definition.IntegratorID,
			definition.EntityType,
			definition.AttributeID,
			true,
			definition.CreatedAt,
			definition.CreatedBy,
			definition.DefaultValue,
			definition.Description,
			definition.EntitlementFeatureName,
			definition.IsHidden,
			definition.Localizations,
			definition.Name,
			definition.AttributeType,
			definition.UIOptions,
			definition.UpdatedAt,
			definition.UpdatedBy,
			definition.ValidationOptions).Exec(); err != nil {
			log.Fatal("Error while trying to save to DB: ", err)
			return err
		}
	}
	return nil
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

func writeIDToFile(productID []VendorAttributeDefinitions) error {
	file, err := os.Create("definitions.txt")
	if err != nil {
		log.Fatal(err)
		return err
	}
	for _, item := range productID {
		_, err = io.WriteString(file, item.IntegratorID.String()+"\n")
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}
func (s *Storage) getDefinitionsFromDB(integratorIDs []products) ([]VendorAttributeDefinitions, error) {
	var defenitions []VendorAttributeDefinitions

	for _, integratorID := range integratorIDs {
		iter := s.db.Query(`SELECT * FROM vendor_attribute_definitions WHERE integrator_id = ?`, integratorID.productID).Iter().Scanner()
		for iter.Next() {
			var defenition VendorAttributeDefinitions
			err := iter.Scan(
				&defenition.IntegratorID,
				&defenition.EntityType,
				&defenition.AttributeID,
				&defenition.Active,
				&defenition.CreatedAt,
				&defenition.CreatedBy,
				&defenition.DefaultValue,
				&defenition.Description,
				&defenition.EntitlementFeatureName,
				&defenition.IsHidden,
				&defenition.Localizations,
				&defenition.Name,
				&defenition.AttributeType,
				&defenition.UIOptions,
				&defenition.UpdatedAt,
				&defenition.UpdatedBy,
				&defenition.ValidationOptions,
			)
			if err != nil {
				log.Fatal(err)
			}
			defenitions = append(defenitions, defenition)
		}

	}

	return defenitions, nil
}

func SetupStorage() (*Storage, error) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "platform_attribute_retrieval_db"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		return &Storage{}, err
	}
	return &Storage{db: session}, nil
}

func (s *Storage) clearData() error {
	if err := s.db.Query(`TRUNCATE platform_attribute_retrieval_db.vendor_attribute_definitions`).Exec(); err != nil {
		return err
	}
	return nil
}

// Localizations list of attribute definition translations
type Localizations []Localization

// Localization translation of fields for the given language
type Localization struct {
	Language    string `json:"language" db:"language"`
	Name        string `json:"name" db:"name"`
	Description string `json:"description" db:"description"`
}

// VendorAttributeDefinitions holds the information about vendor's attribute definition
type VendorAttributeDefinitions struct {
	IntegratorID           gocql.UUID    `db:"integrator_id" cql:"integrator_id"`
	EntityType             string        `db:"entity_type" cql:"entity_type"`
	AttributeID            gocql.UUID    `db:"attribute_id" cql:"attribute_id"`
	Name                   string        `db:"name" cql:"name"`
	Description            string        `db:"description" cql:"description"`
	AttributeType          string        `db:"type" cql:"type"`
	ValidationOptions      string        `db:"validation_options" cql:"validation_options"`
	DefaultValue           *string       `db:"default_value" cql:"default_value"`
	IsHidden               *bool         `db:"is_hidden" cql:"is_hidden"`
	Localizations          Localizations `db:"localizations" cql:"localizations"`
	CreatedAt              time.Time     `db:"created_at" cql:"created_at"`
	UpdatedAt              time.Time     `db:"updated_at" cql:"updated_at"`
	CreatedBy              string        `db:"created_by" cql:"created_by"`
	UpdatedBy              string        `db:"updated_by" cql:"updated_by"`
	Active                 bool          `db:"active" cql:"active"`
	UIOptions              string        `db:"ui_options" cql:"ui_options"`
	EntitlementFeatureName string        `db:"entitlement_feature_name" cql:"entitlement_feature_name"`
}

type products struct {
	productID string `json:"product_id"`
}
type Storage struct {
	db *gocql.Session
}
