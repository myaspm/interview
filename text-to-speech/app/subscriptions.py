"""
This module is for initiating subscriptions.
Please import your Rx Observers and add to appropriate list.
"""

from main import do_logic

# Change only here
REQUEST_SUBSCRIBERS = [do_logic]
MAIN_LOGIC_SUBSCRIBERS = []
ERROR_SUBSCRIBERS = []


def init_subscriptions(
    request_subject, main_logic_subject=None, error_subject=None
):

    for s in REQUEST_SUBSCRIBERS:
        request_subject.subscribe(s)

    if main_logic_subject:
        for s in MAIN_LOGIC_SUBSCRIBERS:
            main_logic_subject.subscribe(s)

    if error_subject:
        for s in ERROR_SUBSCRIBERS:
            error_subject.subscribe(s)
