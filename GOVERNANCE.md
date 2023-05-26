# Fluentd Governance

## Principles

The Fluentd community adheres to the following principles:

- Open: Fluentd is open source. See repository guidelines and CLA, below.
- Welcoming and respectful: See Code of Conduct, below.
- Transparent and accessible: Work and collaboration are done in public.
- Merit: Ideas and contributions are accepted according to their technical merit and alignment with project objectives, scope, and design principles.

## Voting

The Fluentd project employs "Organization Voting" and "Vetoes" to ensure no single organization can dominate the project.

For formal votes, we follow these steps.

1. Have enough period to discuss the topic so that all discussion members agree that there are no more points to be discussed.
2. Add a specific statement of what is being voted on to the description of the relevant GitHub issue or pull request.
3. Declare the start of voting.
4. Ask maintainers to indicate their yes/no votes on the issue or PR.
5. Tally the votes and note the outcomes after a suitable period.

All the following conditions must be satisfied for the vote to be approved.

- No effective objection ballot: See Vetoes, below.
- At least effective __2-organization__ affirmative vote: See Organization Vote, below.
- At least effective __3-maintainers__ affirmative vote.
- At least __2-week__ for voting.

Please note that the period for voting should depend on the topic. __2-week__ is merely the minimum period to ensure time for all organizations to say, "Wait! We need more discussion!". The more significant the decision's impact is, the longer the period should be.

### Organization Vote

Individuals not associated with or employed by a company or organization are allowed one organization vote. Each company or organization (regardless of the number of maintainers associated with or employed by that company/organization) receives one organization vote.

In other words, if two maintainers are employed by Company X, two by Company Y, two by Company Z, and one maintainer is an un-affiliated individual, a total of four "organization votes" are possible; one for X, one for Y, one for Z, and one for the un-affiliated individual.

Any maintainer from an organization may cast the vote for that organization.

### Vetoes

The proposal is not approved as long as any maintainer votes an effective objection ballot.

The maintainer who votes an objection ballot must explain the reason for the objection. Without reasonable justification, the objection ballot is not considered effective.

The ballot can be changed during the voting period. For example, if the reason for the objection is solved by discussion or additional fixes, the objection ballot will be withdrawn and changed to an affirmative vote.

## Changes in Maintainership

New maintainers are proposed by an existing maintainer and are elected by the formal voting process: See Voting, above.

Maintainers can be removed by the formal voting process: See Voting, above.

## Github Project Administration

Maintainers will be added to the __fluent__ GitHub organization and added to the GitHub cni-maintainers team, and made a GitHub maintainer of that team.

After 6 months a maintainer will be made an "owner" of the GitHub organization.

## Projects

The fluent organization is open to receive new sub-projects under it umbrella. To apply a project as part of the __fluent__ organization, it has to met the following criteria:

- Licensed under the terms of the Apache License v2.0
- Project has been active for at least one year since it inception
- More than 2 contributors
- Related to one or more scopes of Fluentd ecosystem:
  - Data collection
  - Log management
  - Metering
- Be supported by the formal voting process: See Voting, above.

The submission process starts as a Pull Request on Fluentd repository with the required information mentioned above. Once a project is accepted, it's considered a __CNCF sub-project under the umbrella of Fluentd__

## Code of Conduct

Fluentd follows the CNCF Code of Conduct:

https://github.com/cncf/foundation/blob/master/code-of-conduct.md
