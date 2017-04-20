// written by z3nithor, world 747
function RunSimV3(){ 
  var fleet = {}, setupSS = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Setup');
  var simParams = setupSS.getRange('P1:P5').getValues(),
	  primusParams = setupSS.getRange('J1:J4').getValues(),
	  fleetData = setupSS.getRange('C13:T21').getValues(),
      simCount = 0;
  // Configure Primus
  var primus = {"accumulator":50,
                "isLocked":false,
				"lockedTurns":0,
                "canBlock":true,
				"satkDisabled":{"value":1,"turnsLeft":0}, //0 = true, 1 = false
                "dodgeRate":primusParams[0][0]*100,
                "blockRate":primusParams[1][0]*100,
				"hitRate":primusParams[2][0]*100,
				"pen":primusParams[3][0]*100,
                "damage":[0],
                "timesLocked":[0],
				"lockData":{"0":[],"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"7":[],"8":[],"9":[],"10":[],"11":[],"12":[]}, // if >12, added automatically
                "poison":{"isPoisoned":false,"damageRate":0,"turnsLeft":0}
               };
  // Use user-specified values
  var controlParams = {"max_iter":simParams[0][0]*1,
					   "blockWhenLocked":false,
					   "forceUp":simParams[2][0]*100,
					   "blockAmt":simParams[3][0]*1,
					   "summonWasUsed":false,
					   "max_ex_time":150,
					   "startTime":new Date().getTime(),
					   "updateLists":false,
					   "fleetDead":false,
					   "fleetAlive":true
					  };
  // Configure fleet - fleet object referenced by the name of the ship, not the position, in order to allow for ship insertion & removal via S-ATK
  for (var i=0;i<fleetData.length;i++) {
	if (fleetData[i][0]=="") continue;
	fleet[fleetData[i][0].toString().toLowerCase()]={"position":1-0+i-0,
									   "numMisses":[0],
									   "numBlocked":[0],
									   "numSATK":[0],
									   "isDead":false,
									   "damageDealt":[0],
									   "numShots":[0],
									   "name":fleetData[i][0].toLowerCase(),
									   "class":fleetData[i][1].toLowerCase(),
									   "rawATK":fleetData[i][2],
									   "rawSATK":fleetData[i][3],
									   "SATKDamage":fleetData[i][4]*100,
									   "SATKReset":fleetData[i][5],
									   "rawCritChance":fleetData[i][6]*100,
									   "rawCritDamage":fleetData[i][7]*100,
									   "rawPenetration":fleetData[i][8]*100,
									   "rawHitRate":fleetData[i][9]*100,
									   "rawDodge":fleetData[i][10]*100,
									   "rawBlock":fleetData[i][11]*100,
									   "initAccumulator":fleetData[i][12],
									   "lockRate":fleetData[i][13]*100,
									   "deAccumulate":fleetData[i][14],
									   "prbDeAccum":fleetData[i][15]*100,
									   "poisonPercent":fleetData[i][16]*100, //15% => 15 is the stored value
									   "lt":fleetData[i][17].toLowerCase(),
									   "ltCanActivate":true
									  };
	fleet[fleetData[i][0].toString()].isDestroyer = (fleetData[i][1].toLowerCase()=="destroyer") ? true : false;
	if (fleetData[i][0]=="Quasimodo") { // Clone Quasimodo to create the Duomilian ship (not sure how its stats are determined)
		controlParams.summonWasUsed = true;
		fleet["summonShip"]=JSON.parse(JSON.stringify(fleet[fleetData[i][0].toString()])); // copy it, not link it
		fleet["summonShip"].position=0;
		fleet["summonShip"].lt="";
		fleet["summonShip"].class="ranger";
		fleet["summonShip"].turnsLeft=0;
		fleet["summonShip"].initAccumulator=0;
	}
}	
  // Default objects are set, run iterations
  while (((new Date().getTime() - controlParams.startTime)/1000 < controlParams.max_ex_time ) && (simCount<controlParams.max_iter)){
	// Initialize per-iteration quantities
    for (var j in fleet) {
	  fleet[j].accumulator = fleet[j].initAccumulator;
	  fleet[j].isDead=false;
	  fleet[j].ltCanActivate=true;
	  fleet[j].invis={"isInvisible":false,"turnsLeft":0,"from":""};
	  fleet[j].stasis={"isDeathProof":false,"turnsLeft":0};
	  fleet[j].boostFromSatk={"satk":{"perm":0,"temp":0,"tempTurns":0},
							  "atk":{"perm":0,"temp":0,"tempTurns":0},
							  "critChance":{"perm":0,"temp":0,"tempTurns":0},
							  "hitRate":{"perm":0,"temp":0,"tempTurns":0},
							  "pen":{"perm":0,"temp":0,"tempTurns":0},
							  "dodge":{"perm":0,"temp":0,"tempTurns":0},
							  "block":{"perm":0,"temp":0,"tempTurns":0}
							 } ;
	  fleet[j].ltSpecial={"atk":{"perm":0,"temp":0,"tempTurns":0},
						  "satk":{"perm":0,"temp":0,"tempTurns":0},
						  "hitRate":{"perm":0,"temp":0,"tempTurns":0},
						  "dodge":{"perm":0,"temp":0,"tempTurns":0},
						  "block":{"perm":0,"temp":0,"tempTurns":0},
						  "critChance":{"perm":0,"temp":0,"tempTurns":0},
						  "pen":{"perm":0,"temp":0,"tempTurns":0}
						 }
	  fleet[j].damageDealt[simCount]=0; // initialize new damage count
	}
    // Activate passive Lieutenant abilities
    ltStartup_(fleet);
    // Reinitialize Primus
    primus.accumulator = 50;
    primus.damage[simCount]=0; // initialize new damage count
    primus.timesLocked[simCount]=0; // initialize new lock count
    primus.isLocked=false;
    primus.canBlock=true;
    primus.poison={"isPoisoned":false,"damageRate":0,"turnsLeft":0};
	// Perform an iteration
    doIterateV3_(fleet,primus,controlParams,simCount++);
    // Summarize round results for histogram
	if (primus.timesLocked[simCount-1] > 12 ) { // With Elsa+4, can get more than 12 locks in a single round, but lockData only premade with 0-12 as options
		try {
			Logger.log(primus.lockData[primus.timesLocked[simCount-1]].length);
		}
		catch(e){
			primus.lockData[primus.timesLocked[simCount-1]]=[]; // couldn't read the length of a non-existent array, so we create it
		}
    }
	primus.lockData[primus.timesLocked[simCount-1]].push(primus.damage[simCount-1]); // histogram data of number of times locked & the damage per round    
  }
  // iterations complete / timed out, report data
  doReport_(fleet,primus,fleetData,setupSS.getParent());
  if (simParams[4][0]==true) doPlot(fleet,primus,SpreadsheetApp.getActiveSpreadsheet().insertSheet());
}
function makePositionMap_(fleet) {
	// iterate the name-referenced fleet object to create a position-referenced object list 
	var outputObj = {"s1":"","s2":"","s3":"","s4":"","s5":"","s6":"","s7":"","s8":"","s9":""};
	var insertSummon = false;
	for (var p in fleet) {
		if (fleet[p].isDead == false) {
			if (p != "summonShip") {
				outputObj["s"+fleet[p].position.toString()]=p.toString();
			} else {
				insertSummon = true;
			}
		}
	}
	if (insertSummon) { 						// Had to finish all other ships first
		var spot = 1;
		while (fleet["summonShip"].position == 0) {
			if (outputObj["s"+spot.toString()] == "") {
				fleet["summonShip"].position = spot-0; 
			}
			spot++;
		}
		outputObj["s"+fleet["summonShip"].position.toString()]="summonShip";
	}
	return outputObj;
}
function makeAttackOrder_(myShips,positionMap,hasFiredList) {
	var attackingOrder = ["s1","s2","s3","s4","s5","s6","s7","s8","s9"],
		aOut = [];
	for (var a = 0;a<9;a++) { // Loop through 'attackingOrder' list
		var shipName = positionMap[attackingOrder[a]];
		if (shipName != "") {
			if (myShips[shipName].isDead == false) { // make sure the ship isn't already dead
				if (hasFiredList.toString().indexOf(shipName) < 0 ) { // make sure the ship hasn't already fired
					if (a >= hasFiredList.length) {
						/* if the summon is placed in 3 by quasi from space 2, a = 2 with hasFiredList.length = 2 - we expect the summon to fire
						   if the summon is placed in 2 by quasi from space 4, a = 1 with hasFiredList.length = 2 - the summon's firing turn has passed
							thad - quasi - X					thad - quasi - summon
							  X  -  FS   - X		becomes 	  X  -  FS   - X
							  X  - Flynn - Vio					  X  - Flynn - Vio
							thad -   X   - X					thad - summon - X
							quas -  FS   - X		becomes		quas -  FS    - X
							  X  - Flynn - Vio					  X  - Flynn  - Vio
						*/
						aOut.push(shipName);
					}
				}
			}
		}
	}
	return aOut;
}
function makeTargetOrder_(myShips,positionMap) { //output is directly pop()-able
	var targetingOrder = ["s2","s5","s8","s1","s4","s7","s3","s6","s9"],
		tOut = [];
	for (var a = 0;a<9;a++) { // Loop through 'targetingOrder' list 
		var shipName = positionMap[targetingOrder[a]];
		if (shipName != "") {
			if (myShips[shipName].isDead == false) tOut.push(shipName);
		}
	}
	return tOut.reverse();
}
function tryToKillShip_(myShips,targetedShip,primus,simNumber,cParams,isCounter) {
	var isDead = false, rnd = 0;
	if (myShips[targetedShip].isDead == true) {
		isDead = true;
	} else {
		if (isCounter == true) { // Primus counterattack - guaranteed hit, we cannot respond, invisibility doesn't matter
			if (myShips[targetedShip].stasis.isDeathProof == false) {
					isDead = true;
			} else {
				myShips[targetedShip].stasis.isDeathProof = false;
				myShips[targetedShip].stasis.turnsLeft = 0;
				myShips[targetedShip].accumulator+=25;
			}
		} else {												// Primus ATK or S-ATK, we can perhaps respond
			if (myShips[targetedShip].invis.isInvisible == true) {
				isDead = false; 								// Can't kill what can't be seen
			} else {											// Check for dodge!
				myShips[targetedShip].dodgeRate = getFinalStat_(myShips,targetedShip,"dodge")-primus.hitRate;
				rnd = Math.random()*100;
				if (rnd <= myShips[targetedShip].dodgeRate) {	// Primus missed!
					isDead = false;
				} else { 										// Primus hits the target
					if (myShips[targetedShip].stasis.isDeathProof == false) {
						isDead = true;
					} else {
						myShips[targetedShip].stasis.isDeathProof = false;
						myShips[targetedShip].stasis.turnsLeft = 0;
						myShips[targetedShip].accumulator+=25; 
						// Check for block & counter
						if ( primus.accumulator < 100 ) { //Can't counter S-ATK
							var currentBlockRate = getFinalStat_(myShips,targetedShip,"block") - primus.pen;
							rnd = Math.random()*100;
							if (rnd <= currentBlockRate) {
								// Block + Counter Successful. Gain 50 accumulator
								myShips[targetedShip].accumulator += 50;
								myShips[targetedShip].ATK = getFinalStat_(myShips,targetedShip,"atk");
								myShips[targetedShip].critChance = getFinalStat_(myShips,targetedShip,"critChance");
								myShips[targetedShip].critDam = myShips[targetedShip].rawCritDamage-0;
								// Perform attack
								var damage = 0.5*myShips[targetedShip].ATK*(1+cParams.forceUp/100);
								rnd = Math.random()*100;
								if (rnd <= myShips[targetedShip].critChance) damage = damage*myShips[targetedShip].critDam/100;
								damage = Math.floor(damage);
								myShips[targetdShip].damageDealt[simNumber] += damage;
								primus.damage[simNumber] += damage;
							}
						}
					}
				}
			}
		}
		if (isDead == true)	{	// Remove invisibility from other ships if we gave it to them and if 
			switch (myShips[targetedShip].name) { 
				case "akhenaton":
					for (var i in myShips) {
						if (myShips[i].invis.from == "akhe") {
							myShips[i].invis.isInvisible = false;
							myShips[i].invis.turnsLeft = 0;
							myShips[i].invis.from = "";
						}
					}
					break;
				case "izolda":
					for (var i in myShips) {
						if (myShips[i].invis.from == "izolda") {
							myShips[i].invis.isInvisible = false;
							myShips[i].invis.turnsLeft = 0;
							myShips[i].invis.from = "";
						}
					}
					break;
				default:
					break;
			}
		}
	}
	return isDead;
}
function getFinalStat_(fleet,ship,stat) {
	var boost = 0, rawStat = 0, finalStat = 0;
	boost = fleet[ship].boostFromSatk[stat].perm-0 + (fleet[ship].boostFromSatk[stat].tempTurns > 0) ? fleet[ship].boostFromSatk[stat].temp-0 : 0; 	// bonuses from S-ATKs
	boost += fleet[ship].ltSpecial[stat].perm-0 + (fleet[ship].ltSpecial[stat].tempTurns > 0) ? fleet[ship].ltSpecial[stat].temp-0 : 0; 			// bonuses from Lt. effects
	switch (stat) {
		case "atk": 
			rawStat = fleet[ship].rawATK-0;
			finalStat = rawStat*(1+boost-0);
			break;
		case "satk":
			rawStat = fleet[ship].rawSATK-0;
			finalStat = rawStat*(1+boost-0);
			break;
		case "dodge": 
			rawStat = fleet[ship].rawDodge-0;
			finalStat = rawStat-0+boost-0;
			break;
		case "block": 
			rawStat = fleet[ship].rawBlock-0;
			finalStat = rawStat-0+boost-0;
			break;
		case "hitRate": 
			rawStat = fleet[ship].rawHitRate-0;
			finalStat = rawStat-0+boost-0;
			break;
		case "pen":
			rawStat = fleet[ship].rawPenetration-0;
			finalStat = rawStat-0+boost-0;
			break;
		case "critChance":
			rawStat = fleet[ship].rawCritChance-0;
			finalStat = rawStat-0+boost-0;
		default:
			return -100000000;
			break;
	}
	return finalStat-0;
}
function doIterateV3_(fleet,primus,controlParams,simNum) {
	var targetList = [],
		attackList = [],
		didAttack = [],
		currentTarget = "",
		fleetMap = makePositionMap_(fleet);
		controlParams.fleetDead = false;
		controlParams.fleetAlive = true;
	while ((controlParams.fleetDead == false) && (primus.damage[simNum] < 5e10)) { // new condition in case of infinite damage scenarios
		// Kill the summoned ship if it has had its turn already
		if (controlParams.summonWasUsed == true) {
			// Summon ship may exist
			if (fleet["summonShip"].position != 0) {
				if (fleet["summonShip"].turnsLeft < 1) {
					fleetMap["s"+fleet["summonShip"].position]="";
					fleet["summonShip"].position=0;
					fleet["summonShip"].isDead = true;
				}
			}
			fleetMap = makePositionMap_(fleet); // regenerate the position map of the fleet since the summonShip can take a variable position (or vanish!)
		}
		attackList = makeAttackOrder_(fleet,fleetMap,[]).reverse();
		didAttack = []; // No one has fired yet on this turn
		if (attackList.length > 0) {
			var shipName = attackList.pop();
			ltCheck_(fleet,shipName,true);
			switch (shipName.toLowerCase()) {
				case "cryptor":
				case "izolda":
					doAttackV3_(fleet[shipName],primus,controlParams,simNum,fleet,makeAttackOrder_(fleet,fleetMap,''));
					break;
				default:
					doAttackV3_(fleet[shipName],primus,controlParams,simNum,fleet,'');
					break;
			}
			didAttack.push(shipName.toString());
			if (controlParams.updateLists == true) {
				// summoned ship was inserted into the fleet, possibly changing the attack order, and definitely altering the target order
				fleetMap = makePositionMap_(fleet);
				attackList = makeAttackOrder_(fleet,fleetMap,didAttack).reverse();
				controlParams.updateLists = false;
			}
			// Primus firing opportunity
			targetList = makeTargetOrder_(fleet,fleetMap);
			if (targetList.length == 0) {
				controlParams.fleetDead = true;
				controlParams.fleetAlive = false;
			} else { //Have something to shoot at
				// Poison Damage Application
				if (primus.poison.isPoisoned == true) {
					primus.damage[simNum]+=primus.poison.damageRate;
					if (primus.poison.turnsLeft <= 1) {
						primus.poison.isPoisoned = false;
						primus.poison.damageRate = 0;
					}
					primus.poison.turnsLeft = Math.max(0,primus.poison.turnsLeft - 1);
				}
				// Kit+4 check (Fortress-Class Primus Invasion Primus variant)
				if (primus.satkDisabled.turnsLeft < 1) {
					primus.satkDisabled.value=1; // 1 = not disabled, 0 = disabled
					primus.satkDisabled.turnsLeft = 0;
				} else {
					primus.satkDisabled.turnsLeft = Math.max(0,primus.satkDisabled.turnsLeft-1);
				}
				// Lock Check / Clear 
				if (primus.isLocked == true) {
					primus.timesLocked[simNum]++;
					primus.isLocked = false;
				} else {
					primus.canBlock = true;
					if (primus.accumulator*primus.satkDisabled.value >= 100) {
						// S-ATK
						controlParams.fleetAlive = false; // Make a probably-correct assumption
						for (var i in fleet) {
							fleet[i].isDead = tryToKillShip_(fleet,i,primus,simNum,controlParams,false);
							controlParams.fleetAlive = controlParams.fleetAlive || (!fleet[i].isDead); // Check assumption
						}
						controlParams.fleetDead = (!controlParams.fleetAlive);
					} else {
						// Regular ATK
						var ship = targetList.pop();
						fleet[ship].isDead = tryToKillShip_(fleet,ship,primus,simNum,controlParams,false);
					}
				}
			}
			// Send the rest of the fleet
			while (attackList.length>0) {
				var shipName = attackList.pop();
				ltCheck_(fleet,shipName,true);
				switch (shipName.toLowerCase()) {
					case "cryptor":
					case "izolda":
						doAttackV3_(fleet[shipName],primus,controlParams,simNum,fleet,makeAttackOrder_(fleet,fleetMap,''));
						break;
					default:
						doAttackV3_(fleet[shipName],primus,controlParams,simNum,fleet,'');
						break;
				}
				didAttack.push(shipName.toString());
				if (controlParams.updateLists == true) {
					fleetMap = makePositionMap_(fleet);
					attackList = makeAttackOrder_(fleet,fleetMap,didAttack).reverse();
					controlParams.updateLists = false;
				}
			}
		} else { // attackList is empty, so our fleet is dead since this check was before any ships have fired
			controlParams.fleetDead = true;
			controlParams.fleetAlive = false;
		}
	}
}
function doAttackV3_(attacker,primus,cParams,simNumber,fleet,attackerList) {
	var rnd = 0,		blockRate = 0,		shotBlocked = false,		shotCountered = false,		isSatk = false,
		damage = 0;
	if (attacker.isDead == true) return;
	attacker.numShots[simNumber]++;
	// Assemble attacker's ATK, SATK, Crit, CritDmg, Penetration, and HitRate abilities. Must be done each attack due to turn-based boost possibility
	attacker.ATK = getFinalStat_(myShips,targetedShip,"atk");
	attacker.SATK = getFinalStat_(myShips,targetedShip,"satk");
	attacker.pen = getFinalStat_(myShips,targetedShip,"pen");
	attacker.hitRate = getFinalStat_(myShips,targetedShip,"hitRate")-primus.dodgeRate;
	attacker.critChance = getFinalStat_(myShips,targetedShip,"critChance");
	attacker.critDam = attacker.rawCritDamage-0;
	// Do we hit Primus?
	rnd = Math.random()*100;
	if (rnd > attacker.hitRate) {
		attacker.numMisses[simNumber]++;
	} else {
		// Hit, but blocked?
		blockRate = (primus.blockRate-attacker.pen)*primus.canBlock;
		rnd = Math.random()*100;
		if (rnd <= blockRate) {
			shotBlocked = true;
			attacker.numBlocked[simNumber]++;
		}
		// Compute damage output
		if (attacker.accumulator > 99) {
			isSatk = true;
			attacker.numSATK[simNumber]++;
		}
		damage = (attacker.ATK-0 + isSatk*attacker.SATK)*(1+forceUp/100)*(1+isSatk*(attacker.SATKDamage/100 - 1))*(1+isSatk*(attacker.accumulator-100)/100);
		rnd = Math.random()*100;
		if (rnd <= attacker.critChance) damage = damage*(attacker.critDam/100); // Add crit damage, if relevant
		if (shotBlocked) damage = damage*cParams.blockAmt;
		damage=Math.floor(damage);
		// Record damage
		primus.damage[simNumber] += damage-0;
		attacker.damageDealt[simNumber] += damage-0;
		attacker.accumulator = (isSatk) ? attacker.SATKReset : attacker.accumulator + 25;
		// S-Attack effects
		if (isSatk) {
			do_satk_effects(attacker, fleet, primus)
            // assess Primus lock, accumulator, poison
			rnd = Math.random()*100;
			if (rnd <= attacker.lockRate) {
				primus.isLocked = true;
				primus.canBlock = false;
				primus.lockedTurns = (attacker.name.toLowerCase() == "velarath") ? 2 : 1;
			}
			rnd = Math.random()*100;
			if (rnd <= attacker.prbDeAccum) {
				primus.accumulator = Math.max(0,primus.accumulator-attacker.deAccumulate);
			}
			if (attacker.poisonPercent > 0) {
				primus.poison.isPoisoned = true;
				primus.poison.damageRate = attacker.poisonPercent/100*(1+cParams.forceUp/100);
				primus.poison.damageRate = primus.poison.damageRate*(attacker.ATK-0 + attacker.SATK-0);
				primus.poison.turnsLeft = 2;
			}
		} else {
			// assess counter, since not an s-atk
			if (shotBlocked) {
				if (attacker.isDestroyer == true || (!primus.canBlock)) {
				} else {
					if (tryToKillShip_(fleet,attacker,primus,simNumber,cParams,true) == true ) {
						attacker.isDead = true;
						primus.accumulator += 25;
					}
				}
			}
		} // end S-ATK effects codes
	} // end we-tried-to-hit-the-primus codes
	// removal of turn-based effects
	for (var i in attacker.ltSpecial) {
		if (attacker.ltSpecial[i].tempTurns == 0) attacker.ltSpecial[i].temp = 0;
		attacker.ltSpecial[i].tempTurns = Math.max(0,attacker.ltSpecial[i].tempTurns-1)
	}
	for (var i in attacker.boostFromSatk) {
		if (attacker.boostFromSatk[i].tempTurns == 0) attacker.boostFromSatk[i].temp = 0;
		attacker.boostFromSatk[i].tempTurns = Math.max(0,attacker.boostFromSatk[i].tempTurns-1)
	}
	if (attacker.invis.turnsLeft == 0) {
		attacker.invis.isInvisible = false;
		attacker.invis.from = "";
	}
	attacker.invis.turnsLeft = Math.max(0,attacker.invis.turnsLeft-1);
	if (attacker.stasis.turnsLeft == 0) attacker.stasis.isDeathProof = false;
	attacker.stasis.turnsLeft = Math.max(0,attacker.stasis.turnsLeft-1);
}