# Contributing to Gonymizer

## Before reporting an issue...

### If you...

 - need help setting up Gonymizer
 - can't figure out something
 - are not sure what's going on or what your problem is

Then please do not open an issue here yet - you should first try one of the following support forums:

 - Slack: https://smith-oss.slack.com

## Reporting an issue properly

By following these simple rules you will get better and faster feedback on your issue.

 - search the bugtracker for an already reported issue

### If you found an issue that describes your problem:

 - please read other user comments first, and confirm this is the same issue: a given error condition might be indicative of different problems - you may also find a workaround in the comments
 - please refrain from adding "same thing here" or "+1" comments
 - you don't need to comment on an issue to get notified of updates: just hit the "subscribe" button
 - comment if you have some new, technical and relevant information to add to the case

### If you have not found an existing issue that describes your problem:

 1. create a new issue, with a succinct title that describes your issue:
   - bad title: "It doesn't work with my machine"
   - good title: "Publish fail: 400 error with E_INVALID_DIGEST"
 2. copy the output of:
   - `gonymizer version`
 3. Run `gonymizer` with the `--log-level=DEBUG` option for debug output, and please include a copy of the command and the output.
 4. If relevant, copy your `gonymizer` logs that show the error or any other logs you can supply that would help in recreating the bug.

## Contributing a patch for a known bug, or a small correction

You should follow the basic GitHub workflow:

 1. fork
 2. commit a change
 3. make sure the tests pass
 4. PR

Additionally, you must [sign your commits](https://github.com/docker/docker/blob/master/CONTRIBUTING.md#sign-your-work). It's very simple:

 - configure your name with git: `git config user.name "Real Name" && git config user.email mail@example.com`
 - sign your commits using `-s`: `git commit -s -m "My commit"`

Some simple rules to ensure quick merge:

 - clearly point to the issue(s) you want to fix in your PR comment (e.g., `closes #12345`)
 - prefer multiple (smaller) PRs addressing individual issues over a big one trying to address multiple issues at once. The only exception here is large features that create more code than the change. Larger diffs with discriptive comments
 - if you need to amend your PR following comments, please squash instead of adding more commits
 - if fixing a bug or adding a feature, please add or update the relevant `CHANGELOG.md` entry with your pull request number
   and a description of the change

## Contributing new features

You are heavily encouraged to first discuss what you want to do. You can do so on the irc channel, or by opening an issue that clearly describes the use case you want to fulfill, or the problem you are trying to solve.

If this is a major new feature, you should then submit a proposal that describes your technical solution and reasoning.
If you did discuss it first, this will likely be green lighted very fast. It's advisable to address all feedback on this proposal before starting actual work

Then you should submit your implementation, clearly linking to the issue (and possible proposal).

Your PR will be reviewed by the community, then ultimately by the project maintainers, before being merged.

It's mandatory to:

 - interact respectfully with other community members and maintainers - more generally, you are expected to abide by the [Docker community rules](https://github.com/docker/docker/blob/master/CONTRIBUTING.md#docker-community-guidelines)
 - address maintainers' comments and modify your submission accordingly
 - write tests for any new code

Complying to these simple rules will greatly accelerate the review process, and will ensure you have a pleasant experience in contributing code to the Registry.

## Review and Development notes

- All merges require LGTMs from any 1 of the maintainers (See MAINTAINERS.md)
- We are using tags to indicate release versions. We use the `master` branch as our development branch.  We have scripts under the `scripts/` directory to change the version numbers for the application (see gonymizer/scripts/version_bump.sh).  Hot fixes, minor patches, and full version releases will be following [Semantic Versioning 2.0.0](https://semver.org/#semantic-versioning-200)
