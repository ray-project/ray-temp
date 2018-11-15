from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import ray  # noqa F401


def test_modin_import():
    # The import must be here because python2 is not supported yet
    if sys.version_info[0] == 3:
        import modin.pandas as pd
        frame_data = [1, 2, 3, 4, 5, 6, 7, 8]
        frame = pd.DataFrame(frame_data)
        assert frame.sum().squeeze() == sum(frame_data)
