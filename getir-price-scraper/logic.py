from config import WORKER_ID

from log import log

from playwright.sync_api import sync_playwright
from pandas import DataFrame
from time import sleep

from libs.playwright_utils import parse_products


def process_message(params):
    # main logic entry
    # raise an exception here to mark the message with error_message
    # the message will be marked as success if no exception is raised
    df = DataFrame()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        log.info(f"[Worker: {WORKER_ID}] [Browser Launched]")
        page = browser.new_page()
        page.goto("https://getir.com")
        log.info(f"[Worker: {WORKER_ID}] [Navigated to getir.com]")

        with page.expect_navigation():
            page.click("button:has-text(\"Yeni Ürünler\")")
            log.info(f"[Worker: {WORKER_ID}] [Navigated to products page.]")

        all_categories_list = [
            cat for cat in page.query_selector(
                "[data-testid=\"card\"]"
            ).inner_text().split("\n") if cat not in params.blacklisted_cats
        ]

        for i in all_categories_list:
            with page.expect_navigation():
                page.click("button:has-text(\"{}\")".format(i))
                log.info(f"[Worker: {WORKER_ID}] [Navigated to {i}]")
            category = page.query_selector(
                "[data-testid=\"breadcrumb-item\"]").text_content()
            log.info(
                f"[Worker: {WORKER_ID}] [Parsing products for {category}]"
            )

            handle = page.query_selector_all('article')
            # Crude way to scroll down
            # Sleep is nessecary to let the page load new images.
            for count in range(0, int(len(handle) / 8) + 10):
                page.keyboard.press("PageDown")
                sleep(0.2)
            df = parse_products(df, handle=handle, category=category)
            print()
        log.info(
            f"[Worker: {WORKER_ID}] [Parsing successful. Closing the browser]"
        )
        browser.close()

    log.info(
        f"[Worker: {WORKER_ID}] [Writing to parquet]"
    )

    df.to_parquet("getir_urunler.parquet")

    log.info(
        f"[Worker: {WORKER_ID}] [Parquet ready, job success.]"
    )
