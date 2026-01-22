from pathlib import Path
import sys
import pandas as pd
from faker import Faker
import random
from datetime import datetime
import uuid
import json
import logging

fake = Faker("en_US")

# --- PROJECT ROOT DEFINITION AND ABSOLUTE PATHS ---

# Go one level up to reach the project root (RETAIL_DWH_PROJECT)
# This ensures folders are created relative to the root, regardless of where the script is executed.
PROJECT_ROOT = Path(__file__).resolve().parent

# --- ABSOLUTE PATHS ---
# Using PROJECT_ROOT for all file operations
HISTORY_DIR = PROJECT_ROOT / "history"
CUSTOMER_HISTORY_FILE = HISTORY_DIR / "customers.json"
PRODUCT_HISTORY_FILE = HISTORY_DIR / "products.json"

OUTPUT_DIR = PROJECT_ROOT / "data/unprocessed"

# --- Global Constants and Variables ---
N_RECORDS = 50

# Global variables store DICTs instead of LISTs for efficient access and updates by ID
EXISTING_CUSTOMERS: dict[str, dict] = {}
EXISTING_PRODUCTS: dict[str, dict] = {}

# configure logger so docker can show them
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# initialize logger
logger = logging.getLogger(__name__)


# --- HISTORY FUNCTIONS ---


def generate_customer() -> dict[str, str]:
    """Generates a random customer profile."""
    return {
        "customer_id": str(uuid.uuid4())[:8],
        "first_name": fake.first_name(),
        "city": fake.city(),
        "email": fake.email(),
    }


def generate_product() -> dict[str, str | float]:
    """Generates a random product profile."""
    categories = ["Electronics", "Books", "Clothing", "Food", "Home Goods"]
    return {
        "product_id": "PROD-" + str(random.randint(1000, 9999)),
        "product_name": fake.word().capitalize()
        + " "
        + random.choice(["Pro", "Max", "Lite"]),
        "product_category": random.choice(categories),
        "price": round(random.uniform(10.0, 500.0), 2),
    }


def transform_list_to_dict(records: list[dict], id_key: str) -> dict:
    """Transforms a list of records into a dictionary using id_key as the primary key."""
    return {record[id_key]: record for record in records if record.get(id_key)}


def load_or_initialize_history(file_path: Path) -> dict:
    """Loads data from a JSON file or returns an empty dictionary if not found."""
    if not file_path.exists():
        return {}
    try:
        json_string = file_path.read_text(encoding="utf-8")
        return json.loads(json_string) if json_string.strip() else {}
    except (json.JSONDecodeError, FileNotFoundError, Exception):
        logger.exception("Error loading or decoding %s", file_path.name)
        return {}


def update_history_file(file_path: Path, new_data: dict):
    """Updates the history file: merges with new data and writes back to disk."""
    existing_data = load_or_initialize_history(file_path)
    existing_data.update(new_data)

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        json_string = json.dumps(existing_data, indent=4, ensure_ascii=False)
        file_path.write_text(json_string, encoding="utf-8")

        # log success
        logger.info(
            "Successfully updated and saved %s records to %s.",
            len(existing_data),
            file_path.name,
        )
    except Exception:
        # log error
        logger.exception("FATAL: Failed to write to %s", file_path.name)


def initialize_history(n_customers: int = 10, n_products: int = 5):
    """Initializes customer and product history by loading existing data or generating new entries."""
    global EXISTING_CUSTOMERS, EXISTING_PRODUCTS

    # Create 'history' folder in the project root
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # === 1. CUSTOMERS LOADING OR INITIALIZATION ===
    EXISTING_CUSTOMERS = load_or_initialize_history(CUSTOMER_HISTORY_FILE)
    if not EXISTING_CUSTOMERS:
        new_customers_list = [generate_customer() for _ in range(n_customers)]
        new_customers_dict = transform_list_to_dict(new_customers_list, "customer_id")
        update_history_file(CUSTOMER_HISTORY_FILE, new_customers_dict)
        EXISTING_CUSTOMERS = new_customers_dict

    # === 2. PRODUCTS LOADING OR INITIALIZATION ===
    EXISTING_PRODUCTS = load_or_initialize_history(PRODUCT_HISTORY_FILE)
    if not EXISTING_PRODUCTS:
        new_products_list = [generate_product() for _ in range(n_products)]
        new_products_dict = transform_list_to_dict(new_products_list, "product_id")
        update_history_file(PRODUCT_HISTORY_FILE, new_products_dict)
        EXISTING_PRODUCTS = new_products_dict

    # log success
    logger.info(
        "History initialized: %s clients, %s products.",
        len(EXISTING_CUSTOMERS),
        len(EXISTING_PRODUCTS),
    )


# Call initialization to populate global dictionaries
initialize_history()


# --- DATA GENERATION FUNCTIONS ---


def _imitate_wrong_data(probability: float, correct_value: str | float | int) -> str:
    """Randomly injects 'dirty' data to test pipeline robustness."""
    # Ensure price/quantity return as strings for CSV if not 'dirty'
    correct_value = str(correct_value)
    if random.random() < probability:
        return random.choice(
            ["undefined", str(round(random.random() * 10)), "True", ""]
        )
    return correct_value


def _imitate_customer_change(probability: float, customer_id: str):
    """Simulates a customer attribute change (SCD Type 2), updating global state and file."""
    customer = EXISTING_CUSTOMERS[customer_id]
    if random.random() < probability:
        if random.random() < probability / 2:
            customer["city"] = fake.city()
            change_field = "city"
        else:
            customer["email"] = fake.email()
            change_field = "email"

        logger.info(
            "[%s] SCD TRIGGERED: Customer %s changed %s.",
            datetime.now().strftime("%H:%M:%S"),
            customer_id,
            change_field,
        )
        update_history_file(CUSTOMER_HISTORY_FILE, {customer_id: customer})


def _imitate_product_change(probability: float, product_id: str):
    """Simulates a product price change (SCD Type 2), updating global state and file."""
    product = EXISTING_PRODUCTS[product_id]
    if random.random() < probability:
        product["price"] = round(random.uniform(10.0, 500.0), 2)
        logger.info(
            "[%s] SCD TRIGGERED: Product %s changed price to %s.",
            datetime.now().strftime("%H:%M:%S"),
            product_id,
            product["price"],
        )
        update_history_file(PRODUCT_HISTORY_FILE, {product_id: product})


def generate_sales_data(num_records: int) -> pd.DataFrame:
    """Generates a synthetic sales dataset including random SCD triggers and dirty data."""
    data = []
    customer_ids = list(EXISTING_CUSTOMERS.keys())
    product_ids = list(EXISTING_PRODUCTS.keys())

    for _ in range(num_records):
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)

        # Simulate changes (SCD Type 2)
        _imitate_customer_change(probability=0.05, customer_id=customer_id)
        _imitate_product_change(probability=0.05, product_id=product_id)

        # Retrieve current data from global dictionaries
        customer = EXISTING_CUSTOMERS[customer_id]
        product = EXISTING_PRODUCTS[product_id]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        record = {
            "TRANSACTION_ID": str(uuid.uuid4())[:8],
            "TRANSACTION_DATE": datetime.now().strftime("%Y%m%d"),
            "CUSTOMER_ID": customer["customer_id"],
            "FIRST_NAME": customer["first_name"],
            "CITY": customer["city"],
            "EMAIL": customer["email"],
            "PRODUCT_ID": product["product_id"],
            "PRODUCT_NAME": product["product_name"],
            "PRODUCT_CATEGORY": product["product_category"],
            "PRICE": _imitate_wrong_data(
                probability=0.05, correct_value=product["price"]
            ),
            "QUANTITY": _imitate_wrong_data(
                probability=0.05, correct_value=random.randint(1, 20)
            ),
            "LOAD_TIMESTAMP": timestamp,
        }
        data.append(record)

    return pd.DataFrame(data)


# Main execution block
if __name__ == "__main__":
    print("GENERATOR STARTING...")
    # Create output directory using absolute path
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df_sales = generate_sales_data(N_RECORDS)
    date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Construct final filename using the output directory path
    filename = OUTPUT_DIR / f"raw_sales_{date}.csv"

    df_sales.to_csv(filename, index=False, header=True)

    # log successful file generation
    logger.info(
        "[%s] Generated %s records to %s",
        datetime.now().strftime("%H:%M:%S"),
        N_RECORDS,
        filename,
    )
