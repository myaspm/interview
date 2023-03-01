"""
This module is for initiating subscriptions.
Please import your Rx Observers and add to appropriate list.
"""

from commons.main import do_logic
from commons.event_emitters import emit_event
from libs.schedule import control_scheduler_status

# Change only here
REQUEST_SUBSCRIBERS = [do_logic]
EVENT_SUBSCRIBERS = [emit_event]
ERROR_SUBSCRIBERS = []
LEADER_SUBSCRIBERS = [control_scheduler_status]


def init_subscriptions(
    request_subject,
    event_subject=None,
    leader_subject=None,
    error_subject=None,
):
    """Short summary.

    Parameters
    ----------
    request_subject : Subject
        Description of parameter `request_subject`.
    event_subject : Subject
        Description of parameter `event_subject`.
    error_subject : Subject
        Description of parameter `error_subject`.
    leader_subject : Subject
        Description of parameter `leader_subject`.

    Returns
    -------
    None

    """

    for subscriber in REQUEST_SUBSCRIBERS:
        request_subject.subscribe(subscriber)

    if event_subject:
        for subscriber in EVENT_SUBSCRIBERS:
            event_subject.subscribe(subscriber)

    if error_subject:
        for subscriber in ERROR_SUBSCRIBERS:
            error_subject.subscribe(subscriber)

    if leader_subject:
        for subscriber in LEADER_SUBSCRIBERS:
            leader_subject.subscribe(subscriber)
