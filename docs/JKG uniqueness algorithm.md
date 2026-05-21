# JKG uniqueness algorithm

In general, a JKG JSON is constructed iteratively: information from multiple
sets of JKGEN files are integrated into a JKG JSON file in series.

Adherence to the JKG Schema means that the nodes of a JKG JSON are unique.
Uniqueness is of particular concern for nodes of string values, such as 
for Term node objects.

# node objects 
## Source nodes
The possible case in which the source for a JKGEN already exists in the JKG JSON is an error--i.e., 
an attempt to ingest a SAB more than once.
**jkgen2jkg** will fail with an error.

## Node_Label nodes
Node_Label types are defined by: 
* the JKG schema, which defines the basic enumeration of [`Source`,`Node_Label`,`Rel_Label`,`Concept`,`CODE`]
* the UMLS, which compiles a list of types from Semantic Network semantic types
The **jkgen2jkg** script does not check Node_Labels for uniqueness.

## Rel_Label nodes
The string _values_ for Rel_Label nodes are unique.

To identify new Rel_Label values from JKGEN, **jkgen2jkg** compares the 
_predicate_ string from the JKGEN edge against the _rel_label_
property of existing Rel_Label objects. 
If a predicate from JKGEN matches an existing _Rel_Label_ object, 
the predicate will be treated as already existing in JKG JSON.

There is a risk of lost information for a SAB
that has an edge predicate that has a meaning different from a similar
rel_label that is already in JKG JSON. For example, if _part_of_ is
already in JKG JSON when a SAB is ingested, _part_of_ will mean what
it means in JKG JSON--not necessarily what _part_of_ means in the SAB.

The JKG generation framework uses standardized relation labels from either UMLS or 
the Relations Ontology when possible. 
The risk of a semantic conflict is low--e.g., _part_of_ is likely to
have the same meaning across SABs. 
The risk of semantic conflict is outweighed by the benefit of avoiding unncessarily 
different versions of the same relationship--e.g., a "ABC:part_of" relationship from SAB ABC  
really means the same as the standard "part_of" relationship.

## Concept nodes
A Concept node for a concept defined in a SAB's JKGEN will be created only if the concept
does not already exist in JKG JSON.

## Term nodes
Values for Term nodes are obtained from two fields of the JKGEN node file:
* _node_label_, which corresponds to both the preferred term of a concept and the preferred term (_tty_=**PT**) for a coderel (CODE relationship)
* _node_synonyms_, a pipe-delimited array of strings that corresponds to synonym terms for coderels (_tty_=**SY**)

In general, nodes in a node file will share synonyms and possibly even labels.

**jkgen2jkg** creates a Term node for field values from the node file if the following are true:
* The value of _node_label_ does not already correspond to a Term node in the JKG JSON.
* A value in _node_synonyms_ does not correspond to either
  * an existing Term node in the JKG JSON
  * a new _node_label_ from the JKGEN

# rel objects
## coderels (CODE rels)
**jkgen2jkg** creates a new _coderel_ object (rel for which _label_=CODE) only 
if a coderel does not already exist in JKG JSON.

## rels (non-CODE rels)
Rel objects that do not represent codes in JKG are built from the edge file of a SAB's JKGEN.
These rels will be unique in a SAB.
