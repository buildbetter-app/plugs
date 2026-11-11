package lexicon

// Exact compiled AST contexts and decoded literal digests; no file exemptions.
var undoLiteralBindings = map[string][]undoLiteralBinding{
	"internal/integration/nightlyrestore/native_diff_test.go": {
		{name: "logNightlyNativeJournal", kind: "func", contextSHA256: "4b73dc1c71285d7dfa32e744a2d8d9e0b752dcb24becded2e11b26c722386960", literals: map[int]string{
			237: "91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75",
		}},
	},
	"internal/storage/physicalcapture/working_test.go": {
		{name: "currentOnlyFamilies", kind: "var", contextSHA256: "808c97eeab1b2bff99cc9a39914b41c01ef803efa8b3f7d70d939f5e781bccfe", literals: map[int]string{
			37:  "91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75",
			58:  "93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26",
			158: "18229c786391bca9892786301aa6e28792948cacc059a8dce6be80cd140a3fa2",
			182: "dc6703a8837e7e8b242f131a64f7ccb0a806c728af4150ef863f2a89c8ad3f57",
			199: "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc",
			220: "5f3c5fb3612b7ea7e7df0cf4f21c4ad123f1603b84d03ad8605fe4b8f67c5025",
			235: "2651723e5b0e7e98fb0a8d09ea1af6d358e7a54df1857e2111f158850c876356",
		}},
	},
	"internal/storage/physicalrestore/restore_test.go": {
		{name: "TestPhysicalRestoreNativeRoundtrip", kind: "func", contextSHA256: "eafe2cb043ce2b2c1dc790f99424c8f1c48839ca9893bd66ff188a750f680df3", literals: map[int]string{
			325:  "6cb1d3f2359ba12e1e39776a16ce2beadb390ea9f051593ba9c3d6f558eb7d94",
			1537: "eafac0152035a28ddd406a0cbac9c742b578398ee312d3e34fae2fa3afcac846",
		}},
		{name: "boundaryDDL", kind: "const", contextSHA256: "122686a1d13bf40dc45c5e366a3aefead0c5ed0239f97e663db1691af44557da", literals: map[int]string{
			20: "b233d5c2f5561d1b592adf515c460570b676a701aa74b3782cc0b1bbf66ecfda",
		}},
		{name: "TestPhysicalRestorePublicLossShapes", kind: "func", contextSHA256: "e614b89d92a7cb47cadefe78d9377fcddbb9e3273fc086bbffda24f8f89e2941", literals: map[int]string{
			782: "35e2cc148f8d7bc2938e258d933308b15676af045a0543142dbdad2e60bceec0",
		}},
	},
	"internal/storage/physicalrestore/undo_test.go": {
		{name: "TestPhysicalUndoActualNativeSchemaWorkingFamilies", kind: "func", contextSHA256: "658c7207b35ae444c29ba91711b4e32982e98037af8602ac4a9069ac11cc83d3", literals: map[int]string{
			476:  "c8831c77027a50042634a662a3f6b88f6ef3993367cdfc2615b1625867fe9b71",
			2439: "b296d4880d41fe371d25c0d9f87373b4719f19a485b83bc4d7062d10f535dd65",
			2765: "362f8a82838430db338b93b87be4275c044937fac24ae52a9e3086e02f430e8c",
		}},
	},
	"internal/testutil/nativeoracle/oracle.go": {
		{name: "issueColumns", kind: "var", contextSHA256: "55943699dfd86c426aa06850c82f0a483ff2033d719500427d6c54a6414402a8", literals: map[int]string{
			19: "6a0ab45152073c64f8c707d0ec2cff73df989feda6681d90366af46dd757f053",
		}},
		{name: "tableColumns", kind: "var", contextSHA256: "5c3026f8e43e9a81ff3e4d653cf59adf3ae869760069c280ed5627f6fde6c7a7", literals: map[int]string{
			275:  "39820d5cac88a8963dae5858ec39b751b85edfc6da7392582ff720ba19b01581",
			396:  "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc",
			425:  "39820d5cac88a8963dae5858ec39b751b85edfc6da7392582ff720ba19b01581",
			594:  "2651723e5b0e7e98fb0a8d09ea1af6d358e7a54df1857e2111f158850c876356",
			710:  "dc6703a8837e7e8b242f131a64f7ccb0a806c728af4150ef863f2a89c8ad3f57",
			879:  "5f3c5fb3612b7ea7e7df0cf4f21c4ad123f1603b84d03ad8605fe4b8f67c5025",
			1163: "18229c786391bca9892786301aa6e28792948cacc059a8dce6be80cd140a3fa2",
			2091: "91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75",
			2182: "93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26",
		}},
	},
	"internal/testutil/nativeschema/cli_migrations.go": {
		{name: "cliCompatibleMigrationSQL", kind: "func", contextSHA256: "8f0e79340ccba4f64125dd96150cccb78cb268a4f3abd4ee8262b3cc6daee54e", literals: map[int]string{
			442:  "6c22e27438c25a2ffecee165c45a22eaceb8a736c01b10b657389a4aa3cc1fe9",
			1104: "f762881f391ae18da2ae27db95ca25ddab8bea3060f89f3f9efb637c50cfdd59",
			1413: "793632f79427734646047c537d60ac840313da7a78515fe2afebc2fd9afe7a03",
		}},
		{name: "cliSubstituteAssumesLocalTables", kind: "func", contextSHA256: "20b4809d4e542bd45d2d32005778716f2b720fb9ccaecf10d58c12118a0f2545", literals: map[int]string{
			78:  "f762881f391ae18da2ae27db95ca25ddab8bea3060f89f3f9efb637c50cfdd59",
			130: "793632f79427734646047c537d60ac840313da7a78515fe2afebc2fd9afe7a03",
		}},
		{name: "cliMigration0065", kind: "const", contextSHA256: "8fbfe3d2aa95e80841f9c081a2974f7b74f15d79e3b5ea5e9c7466833822b972", literals: map[int]string{
			25: "00c6aa3ce5af9ed1093db5d3170c8259b87796832f0d4704b455a295394d7c79",
		}},
		{name: "cliMigration0066AddEventsJournalActor", kind: "const", contextSHA256: "4726265c5157106347de0a3e76b036d114a1c073944b9fa89ac12adc65b7d8db", literals: map[int]string{
			46: "dcb7b5a58d2ba8859089f7dd19e044fbd20dbebc469fdaf48bbe629450c8076d",
		}},
		{name: "cliMigration0041SplitDependenciesTarget", kind: "const", contextSHA256: "0047292073395435715dab54d577965607e135d8ab9e7e49628665def5d791c9", literals: map[int]string{
			48: "102e9b990dc30aa9273a7fbfea94a519237aca03d34540f5efa219a322a197b8",
		}},
		{name: "cliMigration0043DropDependenciesGeneratedColumn", kind: "const", contextSHA256: "0e2cd92dd63e9cbd0b9ea024feece84ca68730e9ff9e85ddb0e090e8e93c0b8f", literals: map[int]string{
			56: "fbd7f35524756c605d40cf83ee44e9f05abd1cb912f55ac04d5324b97d36490c",
		}},
		{name: "cliMigration0053", kind: "const", contextSHA256: "dc798c90e3f99ff2a0a4b3611d9f018eea0b8ca3f07ace54c860016a62e03350", literals: map[int]string{
			25: "a1f0826257613aa1ce97f43dee7d1f5337f830fab55f8f5413aab4f5000de355",
		}},
	},
	"tools/lexicon/native_undo_references.go": {
		{name: "undoSQLReferences", kind: "func", contextSHA256: "52dabdae6a505aea29f36d67babd6711fe4b1a4a2c8538d84a951ab2efc575a3", literals: map[int]string{
			3574:  "4fb1a5584b1bcdd0bf58ac1a76a2c1f97436b067b58e1fde89101ca53bf13228",
			3749:  "929ccb6bd6520c962080943cb00bb56b5ec890903c3f9910ce294e1e5c5e313c",
			5324:  "60a729d80fd7e020f9127470ff6fa2272f3d7bd6d410ad9b5b9821f507461d53",
			5674:  "99f3dd9a86f7a8ed2177f33edb4cde3fececb8c161e1f9322ddf0bc328416139",
			9174:  "ba5010f92bbb451a9ad807e19aca5dc50018a771631ebd8207a5d928ef2878af",
			10049: "0c360c0300cc9b8d0b80b4e5fd84e40ab2f7c7ced92b8c5bbe355b79b8669c31",
			11274: "9c81d60504faa7ddfaafab6eebbef9635be9501d47f073d5be8ce2697b0c07b8",
			12849: "daade5dfb97999c9d7d48b9eca9d26df2cd926b3c1f28e817edbd8542303b3df",
			13024: "9662a8068548eb811bb15697306d038eb7899ec83b180cdb3f66efa292bee94a",
			13199: "8796f8c8bd8b9275900487d74bcdae626135dfddbbb3a58999820efba8053547",
			13374: "c5a052053a98a09d39d1f429a7ffa773557be8cacf76be251c291d0a77a1cedc",
			13549: "dd980665d0c786e1d5fe807bb54d91cee9632d2a331daa0669bc138c7be6ad4c",
			13724: "d8ba2ceed53a702322d0327c7a5cf8ef3ac75bf3fd1b11848dd9e6371f2a2681",
			13899: "8ecbf048baeaeb83905dcef1a9b4251c5f6ec6d8d677fabd6d8315173237e543",
			14249: "7d228662a42ca7668fba4adbcfcc99de46486be628a3a4a18e120a2084bb9c6e",
			14774: "774ad937fc5d310aa1c178f0d1d4abc0649f2463c7a3dfb42c9b79085daeb850",
			14949: "fdd7609253d74f3b15d89b3ce97a8be462e816fe7fc71018be22154b79e48944",
			15124: "eeeedb138831035d5161413638a4a579555b02d1a98e1b596604aff76467eca8",
			15999: "3351d7af5386b0596f31913a0090dbda2ea4ab64ea329eb3a8d3306b26af59bd",
			16174: "08a70e326f4b3fb6e4846089a4b9b4041ad6621a176715694ecc0e42ca63af51",
			16699: "b228df4e38408fecee4b36c8cde3200f4f347bc593e0afaac06ac58449ed6fd5",
		}},
	},
}
