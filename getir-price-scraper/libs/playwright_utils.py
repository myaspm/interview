from config import WORKER_ID

from log import log


def parse_products(df, handle=None, category=None):
    if not category and not handle:
        raise Exception

    for item in handle:
        product_id = None
        text_list = [text for text in item.inner_text().split("\n") if
                     text]
        if "2021 Getir" in text_list[0]:
            continue
        if "product" in item.inner_html():
            product_id = item.inner_html().split("/product/")[1].split("_")[0]
        else:
            pass
        if len(text_list) == 4:
            text_dict = {"id": product_id if product_id else None,
                         "category": category,
                         "name": text_list[2],
                         "old_price": text_list[0],
                         "new_price": text_list[1],
                         "quantity": text_list[3]}
        elif len(text_list) == 3:
            text_dict = {"id": product_id if product_id else None,
                         "category": category,
                         "name": text_list[1],
                         "old_price": "",
                         "new_price": text_list[0],
                         "quantity": text_list[2]}
        else:
            text_dict = {"id": product_id if product_id else None,
                         "category": category,
                         "name": text_list[1],
                         "old_price": "",
                         "new_price": text_list[0],
                         "quantity": ""}

        df = df.append(text_dict, ignore_index=True)

    log.info(f"[Worker: {WORKER_ID}] [{category} productes parsed.]")

    return df

