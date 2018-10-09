# Change Log

## [1.6.2](https://github.com/TheHive-Project/elastic4play/tree/1.6.2) (2018-09-25)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.6.1...1.6.2)

**Implemented enhancements:**

- Add a better filter on attributes in AuxSrv [\#69](https://github.com/TheHive-Project/elastic4play/issues/69)

**Fixed bugs:**

- Make certificate field case insensitive [\#68](https://github.com/TheHive-Project/elastic4play/issues/68)

## [1.6.1](https://github.com/TheHive-Project/elastic4play/tree/1.6.1) (2018-08-27)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.6.0...1.6.1)

**Implemented enhancements:**

- X509 authentication: request certificate without requiring it [\#65](https://github.com/TheHive-Project/elastic4play/issues/65)
- Make SSL truststore configuration optional [\#64](https://github.com/TheHive-Project/elastic4play/issues/64)

**Fixed bugs:**

- GroupByTime on nested fields doesn't work [\#66](https://github.com/TheHive-Project/elastic4play/issues/66)

## [1.6.0](https://github.com/TheHive-Project/elastic4play/tree/1.6.0) (2018-08-20)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.5.0...1.6.0)

**Implemented enhancements:**

- New TheHive-Project repository [\#58](https://github.com/TheHive-Project/elastic4play/issues/58)
- Elasticsearch secured by SearchGuard [\#53](https://github.com/TheHive-Project/elastic4play/issues/53)

**Fixed bugs:**

- Race condition when an attachment is saved [\#63](https://github.com/TheHive-Project/elastic4play/issues/63)
- Session cookie expiration is not correctly checked [\#62](https://github.com/TheHive-Project/elastic4play/issues/62)
- x.509 PKI - illegal object in getInstance: org.bouncycastle.asn1.DERTaggedObject [\#61](https://github.com/TheHive-Project/elastic4play/issues/61)
- Entity rename in migration doesn't work [\#60](https://github.com/TheHive-Project/elastic4play/issues/60)
- Temporary files cannot be created on Windows as their filename contains ":" [\#59](https://github.com/TheHive-Project/elastic4play/issues/59)

**Closed issues:**

- SSL support [\#56](https://github.com/TheHive-Project/elastic4play/issues/56)
- Single Sign-On with X.509 certificates [\#26](https://github.com/TheHive-Project/elastic4play/issues/26)

**Merged pull requests:**

- Add SSL support [\#57](https://github.com/TheHive-Project/elastic4play/pull/57) ([srilumpa](https://github.com/srilumpa))

## [1.5.0](https://github.com/TheHive-Project/elastic4play/tree/1.5.0) (2018-03-29)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.6...1.5.0)

**Implemented enhancements:**

- Get version of ElasticSearch cluster [\#51](https://github.com/TheHive-Project/elastic4play/issues/51)
- Stream is not cluster ready [\#41](https://github.com/TheHive-Project/elastic4play/issues/41)

**Closed issues:**

- Add ability to provide multiple roles on controller helper [\#52](https://github.com/TheHive-Project/elastic4play/issues/52)
- OAuth2 Single Sign-on support [\#42](https://github.com/TheHive-Project/elastic4play/issues/42)

**Merged pull requests:**

- Add support for OAuth2 SSO [\#43](https://github.com/TheHive-Project/elastic4play/pull/43) ([saibot94](https://github.com/saibot94))

## [1.4.6](https://github.com/TheHive-Project/elastic4play/tree/1.4.6) (2018-03-29)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.5...1.4.6)

**Implemented enhancements:**

- Add the ability to remove datastore entry [\#54](https://github.com/TheHive-Project/elastic4play/issues/54)

## [1.4.5](https://github.com/TheHive-Project/elastic4play/tree/1.4.5) (2018-03-08)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.4...1.4.5)

**Fixed bugs:**

- Meta attributes are filtered when entities are converted to json [\#50](https://github.com/TheHive-Project/elastic4play/issues/50)

## [1.4.4](https://github.com/TheHive-Project/elastic4play/tree/1.4.4) (2018-02-08)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.3...1.4.4)

**Fixed bugs:**

- Version of document is not retrieve [\#49](https://github.com/TheHive-Project/elastic4play/issues/49)

## [1.4.3](https://github.com/TheHive-Project/elastic4play/tree/1.4.3) (2018-02-08)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.2...1.4.3)

**Implemented enhancements:**

- Add settings in index creation [\#47](https://github.com/TheHive-Project/elastic4play/issues/47)
- Make migration streams configurable [\#46](https://github.com/TheHive-Project/elastic4play/issues/46)
- Manage concurrent updates [\#44](https://github.com/TheHive-Project/elastic4play/issues/44)

**Fixed bugs:**

- getEntity of migration service doesn't use the right index [\#48](https://github.com/TheHive-Project/elastic4play/issues/48)

## [1.4.2](https://github.com/TheHive-Project/elastic4play/tree/1.4.2) (2018-01-10)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.1...1.4.2)

**Merged pull requests:**

- Add XPack authentication support [\#39](https://github.com/TheHive-Project/elastic4play/pull/39) ([srilumpa](https://github.com/srilumpa))

## [1.4.1](https://github.com/TheHive-Project/elastic4play/tree/1.4.1) (2017-12-07)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.4.0...1.4.1)

**Fixed bugs:**

- Error when configuring multiple ElasticSearch nodes [\#38](https://github.com/TheHive-Project/elastic4play/issues/38)

## [1.4.0](https://github.com/TheHive-Project/elastic4play/tree/1.4.0) (2017-12-05)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.3.2...1.4.0)

**Implemented enhancements:**

- Add query inside aggregations [\#36](https://github.com/TheHive-Project/elastic4play/issues/36)
- Remove the deprecated "user" attribute [\#33](https://github.com/TheHive-Project/elastic4play/issues/33)
- Add the ability to describe attributes of an entity [\#32](https://github.com/TheHive-Project/elastic4play/issues/32)

**Fixed bugs:**

- Query on numeric value doesn't work [\#37](https://github.com/TheHive-Project/elastic4play/issues/37)

## [1.3.2](https://github.com/TheHive-Project/elastic4play/tree/1.3.2) (2017-10-24)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.3.1...1.3.2)

**Fixed bugs:**

- Aggregation on sub-field doesn't work [\#35](https://github.com/TheHive-Project/elastic4play/issues/35)

## [1.3.1](https://github.com/TheHive-Project/elastic4play/tree/1.3.1) (2017-09-18)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.3.0...1.3.1)

**Fixed bugs:**

- Text attribute can't be aggregated nor sorted [\#31](https://github.com/TheHive-Project/elastic4play/issues/31)

## [1.3.0](https://github.com/TheHive-Project/elastic4play/tree/1.3.0) (2017-09-11)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.2.1...1.3.0)

**Implemented enhancements:**

- Add method to query ElasticSearch cluster health [\#30](https://github.com/TheHive-Project/elastic4play/issues/30)
- Rename authentication type by authentication provider [\#29](https://github.com/TheHive-Project/elastic4play/issues/29)
- Add configuration to disable authentication methods [\#28](https://github.com/TheHive-Project/elastic4play/issues/28)
- Add API key authentication type [\#25](https://github.com/TheHive-Project/elastic4play/issues/25)
- Remove defined user roles [\#24](https://github.com/TheHive-Project/elastic4play/issues/24)
- Add support of ElasticSearch 5 [\#11](https://github.com/TheHive-Project/elastic4play/issues/11)

**Fixed bugs:**

- Handle search query error [\#27](https://github.com/TheHive-Project/elastic4play/issues/27)

**Closed issues:**

- Update Play to 2.6 and Scala to 2.12 [\#23](https://github.com/TheHive-Project/elastic4play/issues/23)

## [1.2.1](https://github.com/TheHive-Project/elastic4play/tree/1.2.1) (2017-08-14)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.2.0...1.2.1)

**Fixed bugs:**

- Typo on database check [\#22](https://github.com/TheHive-Project/elastic4play/issues/22)

## [1.2.0](https://github.com/TheHive-Project/elastic4play/tree/1.2.0) (2017-06-30)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.5...1.2.0)

**Implemented enhancements:**

- Add ability to create different document types in stream sink [\#21](https://github.com/TheHive-Project/elastic4play/issues/21)
- Add an API to check if a dblist item exists [\#20](https://github.com/TheHive-Project/elastic4play/issues/20)
- Add method to update a dblist [\#19](https://github.com/TheHive-Project/elastic4play/issues/19)
- Save attachment from data in memory [\#18](https://github.com/TheHive-Project/elastic4play/issues/18)
- Support of attachment in subattribute [\#17](https://github.com/TheHive-Project/elastic4play/issues/17)
- Add support of custom fields attribute [\#16](https://github.com/TheHive-Project/elastic4play/issues/16)

**Fixed bugs:**

- Object attributes are not checked for mandatory subattributes [\#15](https://github.com/TheHive-Project/elastic4play/issues/15)

## [1.1.5](https://github.com/TheHive-Project/elastic4play/tree/1.1.5) (2017-05-11)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.4...1.1.5)

**Implemented enhancements:**

- Make index creation configurable [\#9](https://github.com/TheHive-Project/elastic4play/issues/9)

**Fixed bugs:**

- Offset is not taken into account if search uses scroll [\#12](https://github.com/TheHive-Project/elastic4play/issues/12)

**Closed issues:**

- Scala code cleanup [\#14](https://github.com/TheHive-Project/elastic4play/issues/14)

## [1.1.4](https://github.com/TheHive-Project/elastic4play/tree/1.1.4) (2017-04-18)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.3...1.1.4)

**Implemented enhancements:**

- Update playframework to 2.5.14 [\#13](https://github.com/TheHive-Project/elastic4play/issues/13)

## [1.1.3](https://github.com/TheHive-Project/elastic4play/tree/1.1.3) (2017-03-07)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.2...1.1.3)

**Implemented enhancements:**

- Permit to filter out unaudited attributes in AuxSrv [\#10](https://github.com/TheHive-Project/elastic4play/issues/10)

**Fixed bugs:**

- Invalidate DBList cache when it is updated [\#8](https://github.com/TheHive-Project/elastic4play/issues/8)

## [1.1.2](https://github.com/TheHive-Project/elastic4play/tree/1.1.2) (2017-01-16)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.1...1.1.2)

**Implemented enhancements:**

- Change date format \(ISO -\> Timestamp\) [\#7](https://github.com/TheHive-Project/elastic4play/issues/7)
- \[Refactoring\] Add global error handler [\#6](https://github.com/TheHive-Project/elastic4play/issues/6)
- \[Refactoring\] Make Fields methods more coherent [\#5](https://github.com/TheHive-Project/elastic4play/issues/5)

## [1.1.1](https://github.com/TheHive-Project/elastic4play/tree/1.1.1) (2016-11-22)
[Full Changelog](https://github.com/TheHive-Project/elastic4play/compare/1.1.0...1.1.1)

**Implemented enhancements:**

- \[Feature\] Add support of attachment input value [\#3](https://github.com/TheHive-Project/elastic4play/issues/3)
- \[Refactoring\] Format Scala code in build process [\#2](https://github.com/TheHive-Project/elastic4play/issues/2)

**Fixed bugs:**

- \[Bug\] Prevent authentication module to indicate if user exists or not [\#4](https://github.com/TheHive-Project/elastic4play/issues/4)
- \[Bug\] Fix the build configuration file [\#1](https://github.com/TheHive-Project/elastic4play/issues/1)

## [1.1.0](https://github.com/TheHive-Project/elastic4play/tree/1.1.0) (2016-11-04)


\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*