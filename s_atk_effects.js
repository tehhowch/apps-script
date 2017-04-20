// S-ATK effects
function do_satk_effects(attacker, fleet, primus) {
    switch (attacker.name.toLowerCase()) {
        case "frost jr":
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost.toString());
            }
            var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
            fleet[acBoostChoices[acBoostRoll]].accumulator+=30;
            break;
        case "cabal":
            // self-invisibility for 2 effective rounds (takes effect after firing)
            attacker.invis.isInvisible = true;
            attacker.invis.from = "cabal";
            attacker.invis.turnsLeft = 2;
            // Assign 2x 20% dodge boosts
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost);
            }
            for (var i = 0;i<2;i++) {
                var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
                var acBoost = acBoostChoices.splice(acBoostRoll,1);
                fleet[acBoost].boostFromSatk.dodge.temp = 20;
                fleet[acBoost].boostFromSatk.dodge.tempTurns = 1;
            }
            break;
        case "dark carter": 		// +35 accum to fleet
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) fleet[acBoost].accumulator+=35;
            }
            break;
        case "opal":				// +10% dodge to fleet
            for (var acBoost in fleet) {
                fleet[acBoost].boostFromSatk.dodge.temp = 10;
                fleet[acBoost].boostFromSatk.dodge.tempTurns = 1;
            }
            break;
        case "akhenaton":
            if (Math.random()*100<10) primus.damage[simNumber]=primus.damage[simNumber]-100000; // added a shield to Primus
            for (var acBoost in fleet) {
                switch (fleet[acBoost].name) {
                    case "akhenaton":
                    case "izolda":
                        break;
                    default:
                        if (!fleet[acBoost].isDead) {
                            fleet[acBoost].invis.isInvisible = true;
                            fleet[acBoost].invis.from = "akhe";
                            fleet[acBoost].invis.turnsLeft = 1;
                        }
                        break;
                }
            }
            break;
        case "carter":
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) fleet[acBoost].accumulator+=50;
            }
            break;
        case "cryptor":
            attacker.stasis.isDeathProof = true;
            attacker.stasis.turnsLeft = 5;
            if (attackerList.length > 1) { // is someone other than just cryptor present?
                attackerList.reverse();
                var acBoost = attackerList.pop();
                if (acBoost == "cryptor") acBoost = attackerList.pop(); // cryptor was in s1
                fleet[acBoost].stasis.isDeathProof = true;
                fleet[acBoost].stasis.turnsLeft = 5;
            } else {
                // cryptor is the last alive, nothing to do
            }
            break;
        case "hunter": 				// 2x 2-turn +20% hitrate buffs
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost);
            }
            for (var i = 0;i<2;i++) {
                var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
                var acBoost = acBoostChoices.splice(acBoostRoll,1);
                fleet[acBoost].boostFromSatk.hitRate.temp = 20;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 2;
            }
            break;
        case "quasimodo": 			// summon a ship
            cParams.updateLists = true;
            fleet['summonShip'].isDead=false;
            fleet['summonShip'].accumulator = 0;
            fleet['summonShip'].position = 0; // trigger insertion mode behavior
            fleet['summonShip'].turnsLeft = 1;
            break;
        case "darrien": 			// hit rate for all, block rate for 3
            for (var acBoost in fleet) { 
                if (!fleet[acBoost].isDead) {
                    fleet[acBoost].boostFromSatk.hitRate.temp = 100;
                    fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
                }
            }
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost);
            }
            for (var i = 0;i<3;i++) {
                var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
                var acBoost = acBoostChoices.splice(acBoostRoll,1);
                fleet[acBoost].boostFromSatk.block.temp = 100;
                fleet[acBoost].boostFromSatk.block.tempTurns = 1;
            }
            break;
        case "dark darrien": 		// 3 random get +HR, 3 random get +blk
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost);
            }
            for (var i = 0;i<3;i++) {
                var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
                var acBoost = acBoostChoices.splice(acBoostRoll,1);
                fleet[acBoost].boostFromSatk.hitRate.temp = 60;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
            }
            var acBoostChoices = [];
            for (var acBoost in fleet) {
                if (!fleet[acBoost].isDead) acBoostChoices.push(acBoost);
            }
            for (var i = 0;i<3;i++) {
                var acBoostRoll = Math.floor(Math.random()*acBoostChoices.length);
                var acBoost = acBoostChoices.splice(acBoostRoll,1);
                fleet[acBoost].boostFromSatk.block.temp = 60;
                fleet[acBoost].boostFromSatk.block.tempTurns = 1;
            }
            break;
        case "lazarus":				// boost to S-ATK by 30% for 1 round
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.satk.temp = 0.3;
                fleet[acBoost].boostFromSatk.satk.tempTurns = 1;
            }
            break;
        case "dark lazarus":		// boost to S-ATK by 25% for 1 round
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.satk.temp = 0.25;
                fleet[acBoost].boostFromSatk.satk.tempTurns = 1;
            }
            break;
        case "roxy":				// crit chance and hit rate, 25% 1 round
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.hitRate.temp = 25;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
                fleet[acBoost].boostFromSatk.critChance.temp = 25;
                fleet[acBoost].boostFromSatk.critChance.tempTurns = 1;
            }
            break;
        case "kit":					// self block shield
            attacker.boostFromSatk.block.temp = 30
            attacker.boostFromSatk.block.tempTurns = 2;
            break;
        case "warden":				// block shields for all
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.block.temp = 30;
                fleet[acBoost].boostFromSatk.block.tempTurns = 1;
            }
            break;
        case "izolda":
            izoldaInvisCheck:
            if (attackerList.length > 1) { // is someone other than just izolda present?
                attackerList.reverse();
                var acBoost = attackerList.pop();
                while (( acBoost == "izolda" || acBoost == "akhenaton" )) {
                    acBoost = attackerList.pop();
                    if (attackerList.length == 0) break izoldaInvisCheck; // we found only izolda and akhenaton, and neither can be made invisible, so we hard-stop in this s-atk effects case
                }
                fleet[acBoost].invis.isInvisible = true;
                fleet[acBoost].invis.turnsLeft = 1;
                fleet[acBoost].invis.from = "izolda";
            } else {
                // izolda is the last alive, nothing to do
            }
            break;
        case "kerom":				// +20% HR to all		
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.hitRate.temp = 20;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
            }
            break;
        case "celeste":				// +100% HR, Crit Chance
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.hitRate.temp = 100;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
                fleet[acBoost].boostFromSatk.critChance.temp = 100;
                fleet[acBoost].boostFromSatk.critChance.tempTurns = 1;
            }
            break;
        case "dark celeste":		// +50% HR, cc
            for (var acBoost in fleet) {
                fleet[acBoost].boostFromSatk.hitRate.temp = 50;
                fleet[acBoost].boostFromSatk.hitRate.tempTurns = 1;
                fleet[acBoost].boostFromSatk.critChance.temp = 50;
                fleet[acBoost].boostFromSatk.critChance.tempTurns = 1;
            }
            break;
        case "gambit": 				// +20% atk for 2 rounds
            for (var acBoost in fleet) { 
                fleet[acBoost].boostFromSatk.atk.temp = 0.2;
                fleet[acBoost].boostFromSatk.atk.tempTurns = 2;
            }
            break;
        default:
            break;
    }
}