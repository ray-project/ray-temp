from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class _NullLogSpan(object):
    """A log span context manager that does nothing"""

    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        pass


NULL_LOG_SPAN = _NullLogSpan()


def profile(event_type, extra_data=None):
    """Profile a span of time so that it appears in the timeline visualization.

    Note that this only works in the raylet code path.

    This function can be used as follows (both on the driver or within a task).

    .. code-block:: python

        with ray.profile("custom event", extra_data={'key': 'value'}):
            # Do some computation here.

    Optionally, a dictionary can be passed as the "extra_data" argument, and
    it can have keys "name" and "cname" if you want to override the default
    timeline display text and box color. Other values will appear at the bottom
    of the chrome tracing GUI when you click on the box corresponding to this
    profile span.

    Args:
        event_type: A string describing the type of the event.
        extra_data: This must be a dictionary mapping strings to strings. This
            data will be added to the json objects that are used to populate
            the timeline, so if you want to set a particular color, you can
            simply set the "cname" attribute to an appropriate color.
            Similarly, if you set the "name" attribute, then that will set the
            text displayed on the box in the timeline.

    Returns:
        An object that can profile a span of time via a "with" statement.
    """
    return NULL_LOG_SPAN  # TODO
