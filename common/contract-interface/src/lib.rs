use ethers::prelude::abigen;

// run `cargo doc -p contract-interface --open` to read struct definition

abigen!(
    IonianFlow,
    "../../ionian-contracts-poc/artifacts/contracts/dataFlow/Flow.sol/Flow.json"
);
