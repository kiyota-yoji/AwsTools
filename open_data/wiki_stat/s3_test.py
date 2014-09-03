#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse, botocore.session
from botocore.exceptions import ProfileNotFound
import preferences as PREFS

class S3Handler():

    def __init__(self, pref_aws):
        self._set_aws_profile(pref_aws['profile_name'])
        print repr(self.aws_profile)

    def _set_aws_profile(self, profile_name):
        """Get AWS profile from the AWS configuration file (~/.aws/config).

        :type profile_name: str
        :param profile_name: The profile name.
        """

        session = botocore.session.Session()
        profile_map = session._build_profile_map()
        if profile_name is None:
            self.aws_profile = profile_map.get('default', {})
        elif profile_name not in profile_map:
            # Otherwise if they specified a profile, it has to
            # exist (even if it's the default profile) otherwise
            # we complain.
            raise ProfileNotFound(profile=profile_name)
        else:
            self.aws_profile = profile_map[profile_name]
        


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='AWS S3 handling test')
    parser.add_argument('-a', '--aws-option', default='default')
    args = parser.parse_args()

    pref_aws = PREFS.AWS[args.aws_option]
    handler = S3Handler(pref_aws)
