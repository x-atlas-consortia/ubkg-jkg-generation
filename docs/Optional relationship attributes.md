# UBKG-JKG: Optional relationship attributes
# Background
It is possible to define optional _attributes_ of a relationship in a JKG instance--i.e., 
* a relationship between Concept nodes
* a _coderel_ relationship between a Concept and a Term node 

A _relationship attribute_ corresponds to a set of properties assigned to a relationship.

# JSON Schema and rules
The [JKG JSON schema](https://github.com/x-atlas-consortia/json-knowledge-graph/blob/main/JKG_Schema.json) specifies optional relationship attributes.

The properties that comprise an optional relationship attribute are related and governed by rules.

### structure
Following is a _properties_ object for a coderel with a _value_ field.
```azure
"properties": 
    {"sab": "MOTRPAC", 
    "def": "", 
    "codeid": "MOTRPAC:ENSRNOG00000000012-gastrocnemius-male", 
    "tty": "PT", 
    "attribute": "", 
    "valuestr": "0.0463164499754571", 
    "value": 0.0463164499754571, 
    "lowerbound": null, 
    "upperbound": null, 
    "unit": ""}
```
### attribute
Identifier for the attribute. 
Currently, the ETL recognizes an "evidence_class" identifier; however, **attribute** can be blank.

The JKG ETL looks for a "evidence_class" column in both the edge and node files:
* If the edge file contains an "evidence_class" column, the ETL assigns the attribute's properties to a non-CODE rel.
* If the node file contains an "evidence_class" column, the ETL assigns the attribute's properties to the CODE rel linking the node's assigned Concept and the node's preferred term--i.e., the Term node linked to the Concept via a coderel with tty="PT".

### valuestr
String value property for the attribute. The value can be:
* blank
* a categorical string value
* the string representation of the number in _value_

### value
Numeric value property for the attribute. Can be null.

### lowerbound
Numeric lower bound property for the attribute value. Can be null.

### upperbound
Numeric upper bound property for the attribute value. Can be null.

### unit
Unit property for the attribute value. Can be blank.

### relationships between properties
1. The _upperbound_, _lowerbound_, and _unit_ properties specify the attribute's _value_.
2. The _attribute_ property describes the entire set of properties.

The ETL script currently does not validate attribute properties--i.e., whether _value_ is 
between _lowerbound_ and _upperbound_.

# Current state of attributes for Data Distillery
SABs that are part of the Data Distillery context partially define optional relationship attributes.
* Some SABs include _evidence_class_ columns in their edge files, but no values for the _value_, _lowerbound_, _upperbound_, or _unit_ columns. 
* Some edge files with _evidence_class_ columns contain no values.
* Some SABs include _value_, _lowerbound_, or _unit_ columns, but no _evidence_class_ column. This corresponds to an undefined relationship attribute.

| SAB       | evidence_class in edge? | evidence_class in node? | value in edge?     | value in node? | Example            | attribute assignment | attribute      |
|-----------|-------------------------|:------------------------|--------------------|:---------------|:-------------------|:---------------------|:---------------|
| CLINGEN   | yes                     | no                      | categorical string | none           | Likely_pathogenic  | rel                  | evidence_class |
| STRING    | yes                     | no                      | integer            | none           | 456                | rel                  | evidence_class |
| GTEXCOEXP | yes                     | no                      | integer            | none           | 6                  | rel                  | evidence_class |
| IDGP      | yes                     | no                      | categorical string | none           | lc50               | rel                  | evidence_class |
| LINCS     | yes                     | no                      | float              | none           | 0.00375706         | rel                  | evidence_class |
| MOTRPAC   | no                      | no                      | no                 | float          | 0.0463164499754571 | coderel              | undefined      |
| HMAZ      | yes                     | no                      | categorical string | none           | l2                 | rel                  | evidence_class |
| OTG       | yes                     | no                      | numeric            | none           | 2.00E-12           | rel                  | evidence_class |

# New attributes
The current ETL only recognizes the "evidence_class" attribute identifier. 
Adding a new attribute type will require the following enhancements:
* JKG JSON schema
* to the ETL to recognize attribute columns during streaming of the JKG JSON
* to the ETL to write attribte sets during writes to the JKG JSON


