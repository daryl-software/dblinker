# Contributing

We love pull requests from everyone.

Fork, then clone the repo:

    git clone git@github.com:your-username/dblinker.git

Make your change. Add tests for your change. Make the tests pass:

    behat

Push to your fork and [submit a pull request][pr].

The pull request will be tested on [Travis][travis] with the supported version of PHP and analyse with [Scrutinizer][scrutinizer]. Make sure all tests are green.

At this point you're waiting on us. We like to at least comment on pull requests within three business days (and, typically, one business day). We may suggest some changes or improvements or alternatives.

Some things that will increase the chance that your pull request is accepted:

* Write tests.
* Follow the [PSR-2][psr2] style guide.
* Write a [good commit message][commit].

[pr]: https://github.com/ezweb/dblinker/compare/
[travis]: https://travis-ci.org/ezweb/dblinker/
[scrutinizer]: https://scrutinizer-ci.com/g/ezweb/dblinker/
[psr2]: http://www.php-fig.org/psr/psr-2/
[commit]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
