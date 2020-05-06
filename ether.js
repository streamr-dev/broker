const {
    Contract,
    ContractFactory,
    utils: { computeAddress, parseEther, formatEther },
    Wallet,
    providers: { Web3Provider, JsonRpcProvider }
} = require("ethers")
const SimpleTrackerRegistry = require("./build/contracts/SimpleTrackerRegistry.json")
const simple_tracker_reg_address = '0xBFCF120a8fD17670536f1B27D9737B775b2FD4CF'
const provider = new JsonRpcProvider('http://localhost:8545');
const str = new Contract(simple_tracker_reg_address, SimpleTrackerRegistry.abi,provider)
async function start() {
    let nodes = await str.getNodes();
    console.log(`nodes : ${JSON.stringify(nodes)}`)
}
start()
