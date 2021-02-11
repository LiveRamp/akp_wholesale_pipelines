# Bigtable expansion tables and their schemas
## UK and FR-specific Expansion Tables
For UK and FR, expansion data is spread out across four tables, all in `com-liveramp-bigtable` instance. These are used for basically all Ingestion requests (exception to come). While they are named clink_email and vice versa, they contain clinks tied to a couple different identifiers.
* emails - md5, sha1, sha256
* addresslinks (alinks)- abilitec household key 
* amkcmk - FR name+postal & household key


### `uk_email_clink`
Rowkey: hashed email (md5, sha1, sha256)
Column Families
 * `ukrema` --  clinks

### `uk_clink_email`
Rowkey: clinks or alinks
Column Families
* `ukrema` -- hashed emails 
*  `ukaol` -- clinks or alinks


### `fr_clink_email`
Rowkey:  clink
Column Families
* `frconcatamkcmk` 
* `frcjt` 
* `frpostcode` 

### `fr_email_clink`
Rowkey: hashed emails (md5)  and amkcmks
Column Families
* `frconcatamkcmk` 
* `frcjt` 
* `frpostcode` 


## Wholesale Table (`uk_wholesale`)
This table is used primarily for wholesale to match PELs to hashed emails. It also is used for UK data append because the hashed emails contained within is properly licensed for data append, whereas the hashed emails in `uk_clink_email`  is not. It is located in the `com-liveramp-eu-wholesale-prod` bigtable instance. 

Rowkey: clinks or PELs
Column Families
* `hashes`  -- 3 column qualifiers `md5`, `sha1`, `sha256`
e.g.
```
0000GB0012AFA1
    hashes:md5:<md5 hash>           @ <timestamp 0>
    hashes:sha1:<sha1 hash>         @ <timestamp 0>
    hashes:sha256:<sha256 hash>     @ <timestamp 0>
------------------------------------------
```



## Generic EU PII Expansion Table (`pii_expansion`)
This table will store PII from German (DE), Italy (IT), and Spain (ES) for use with data append ingestion requests. It lives in the `com-liveramp-bigtable` instance.

#### RowKey: 
`<Region>#<"email" or "phone">#<hash>`
* Differentiating between email and phone in the row key isn't strictly necessary, but may give some insights into access patterns of this data. 

#### Column Families
* `email` -- contains 0...n sha256 hashed emails 
* `phone` -- contains 0...n sha256 hashed phones
	* Will have at least 1 email or phone

#### Column Qualifier
The hashes themselves are used as the column qualifiers, and the value fields will be empty, e.g.
* `cf1:cq1:"", cf1:cq2:""`

#### Timestamp
Timestamp is time of load

```
DE#phone#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3
    email:811786ad1ae74adfdd20dd0372abaaebc6246e343aebd01da0bfc4c02bf0106c:""        @ <timestamp1>
    email:00AA1234abcd4313fDA21234abcd4313fDA21234abcd4313fDA21234abcd4313:""        @ <timestamp1>
    phone:98fb12341234123498fb12341234123498fb12341234123498fb123412341234:""	     @ <timestamp2> 
------------------------------------------
```

### Info about dataflow job
There is a dataflow loader job [here](../dataflow-eu/README.md) is to load email and phone hashes (for now) to the pii_expansion table in BT. This will 
combine the pii hashes for a given customer and store into an expansion table

Example:
 
Email File
```
CustomerId1 EmailHash1 
CustomerId1 EmailHash2
CustomerId2 EmailHash3 
```
Phone File
```
CustomerId1 PhoneHash1
CustomerId2 PhoneHash2
CustomerId2 PhoneHash3
```
Will **effectively** result in: 
``` 
EmailHash1 -> EmailHash2, PhoneHash1
EmailHash2 -> EmailHash1, PhoneHash1
EmailHash3 -> PhoneHash1, PhoneHash2
PhoneHash1 -> EmailHash1, EmailHash2 
PhoneHash2 -> PhoneHash3, EmailHash3 
PhoneHash3 -> PhoneHash2, EmailHash3
```

Where the rowkey is of format: 
```
<REGION>#+<EMAIL/PHONE>#<HASHVALUE>
DE#EMAIL#87924606b4131a8aceeeae8868531fbb9712aaa07a5d3a756b26ce0f5d6ca674
```

