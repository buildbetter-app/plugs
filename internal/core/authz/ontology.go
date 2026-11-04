package authz

// ManageOntology is a distinct protected operation. Neither Modify on a bead
// nor ImportBulkHistory grants an ontology migration.
const ManageOntology Action = "manageOntology"

const Ontology ResourceKind = "ontology"
