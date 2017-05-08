/**
 * @OnlyCurrentDoc
 */
// written by z3nithor, world 747
// Global variables
var primus, fleet, controlParams,  fleetMap

function RunSimV3(){
  var setupSS = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Setup');
  var simParams = setupSS.getRange('P1:P5').getValues(),
      primusParams = setupSS.getRange('J1:J4').getValues(),
      fleetData = setupSS.getRange('C13:V21').getValues(),
      simCount = 0;
  // Configure Primus
  primus = {"accumulator":50,
                "isLocked":false,
                "lockedTurns":0,
                "canBlock":true,
                "satkDisabled":{"value":1, "turnsLeft":0}, //0 = true, 1 = false
                "dodgeRate":primusParams[0][0]*100,
                "blockRate":primusParams[1][0]*100,
                "hitRate":primusParams[2][0]*100,
                "pen":primusParams[3][0]*100,
                "damage":[0],
                "timesLocked":[0],
                "timesHit":[0],
                "timesCountered":[0],
                "satkKills":[0],
                "lockData":{"0":[], "1":[], "2":[], "3":[], "4":[], "5":[], "6":[], "7":[], "8":[], "9":[], "10":[], "11":[], "12":[]}, // if >12, added automatically
                "poison":{"isPoisoned":false, "damageRate":0, "turnsLeft":0},
                "curses":[],
               };
  // Use user-specified values
  controlParams = {"max_iter":simParams[0][0]*1,
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
  fleet = {};
  for ( var i=0;i<fleetData.length;i++){
    if ( fleetData[i][0] == "") continue;
    fleet[fleetData[i][0].toString().toLowerCase()] = {"position":1-0+i-0,
                                                       "numMisses":0,
                                                       "numBlocked":0,
                                                       "numSATK":[0],
                                                       "isDead":false,
                                                       "damageDealt":[0],
                                                       "numShots":[0],
                                                       "accuOnDeath":[0],
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
                                                       "rawDmgUp": fleetData[i][13]*100,
                                                       "rawSDmgUp": fleetData[i][14]*100,
                                                       "lockRate":fleetData[i][15]*100,
                                                       "deAccumulate":fleetData[i][16],
                                                       "prbDeAccum":fleetData[i][17]*100,
                                                       "poisonPercent":fleetData[i][18]*100, //15% => 15 is the stored value
                                                       "lt":fleetData[i][19].toLowerCase(),
                                                       "ltCanActivate":true
                                                      };
    fleet[fleetData[i][0].toString().toLowerCase()].isDestroyer = (fleetData[i][1].toLowerCase() == "destroyer") ? true : false;
    if ( fleetData[i][0].toString().toLowerCase() == "quasimodo"){ // Clone Quasimodo to create the Duomilian ship (not sure how its stats are determined)
      controlParams.summonWasUsed = true;
      var s = JSON.stringify( fleet[fleetData[i][0].toString().toLowerCase()])
      fleet["summonShip"]=JSON.parse( JSON.stringify( fleet[fleetData[i][0].toString().toLowerCase()])); // copy it, not link it
      fleet["summonShip"].position = 0;
      fleet["summonShip"].lt = "";
      fleet["summonShip"].rawATK = 225541; // at level 100, he always does 225541 damage
      fleet["summonShip"].rawDmgUp = 0;
      fleet["summonShip"].rawSDmgUp = 0;
      fleet["summonShip"].class = "ranger";
      fleet["summonShip"].turnsLeft = 0;
      fleet["summonShip"].initAccumulator = 0;
      fleet["summonShip"].name = 'summonShip';
    }
  }
  // Default objects are set, run iterations
  while ((( new Date().getTime() - controlParams.startTime)/1000 < controlParams.max_ex_time) && ( simCount < controlParams.max_iter)){
    // Initialize per-iteration quantities
    for ( var j in fleet){
      fleet[j].accumulator = fleet[j].initAccumulator;
      fleet[j].isDead= (j == 'summonShip') ? true : false;
      fleet[j].ltCanActivate = true;
      fleet[j].invis = {"isInvisible":false, "turnsLeft":0, "from":""};
      fleet[j].immunity = {"isImmune":false, "hitsLeft":0};           // Sturdy
      fleet[j].stasis = {"isDeathProof":false, "turnsLeft":0};         // Cryptor & Louise
      fleet[j].wainFury = {"isLinked":false, "turnsLeft":0, "from":"", "amount":1};
      fleet[j].boostFromSatk = {"satk":{"perm":0, "temp":0, "tempTurns":0},
                                "atk":{"perm":0, "temp":0, "tempTurns":0},
                                "critChance":{"perm":0, "temp":0, "tempTurns":0},
                                "hitRate":{"perm":0, "temp":0, "tempTurns":0},
                                "pen":{"perm":0, "temp":0, "tempTurns":0},
                                "dodge":{"perm":0, "temp":0, "tempTurns":0},
                                "block":{"perm":0, "temp":0, "tempTurns":0},
                                "dmgUp":{"perm":0, "temp":0, "tempTurns":0},
                                "sDmgUp":{"perm":0, "temp":0, "tempTurns":0}
                               } ;
      fleet[j].ltSpecial = {"atk":{"perm":0, "temp":0, "tempTurns":0},
                            "satk":{"perm":0, "temp":0, "tempTurns":0},
                            "hitRate":{"perm":0, "temp":0, "tempTurns":0},
                            "dodge":{"perm":0, "temp":0, "tempTurns":0},
                            "block":{"perm":0, "temp":0, "tempTurns":0},
                            "critChance":{"perm":0, "temp":0, "tempTurns":0},
                            "pen":{"perm":0, "temp":0, "tempTurns":0},
                            "dmgUp":{"perm":0, "temp":0, "tempTurns":0},
                            "sDmgUp":{"perm":0, "temp":0, "tempTurns":0}
                           }
      fleet[j].damageDealt[simCount] = 0; // initialize new counters
      fleet[j].numShots[simCount] = 0;
      fleet[j].numSATK[simCount] = 0;
    }
    // Activate passive Lieutenant abilities
    StartLieutenants_();
    // Reinitialize Primus
    primus.accumulator = 50;
    primus.damage[simCount] = 0; // initialize new damage count
    primus.timesLocked[simCount] = 0; // initialize new lock count
    primus.timesHit[simCount] = 0;
    primus.timesCountered[simCount] = 0;
    primus.satkKills[simCount] = 0;
    primus.isLocked = false;
    primus.canBlock = true;
    primus.lockedTurns = 0;
    primus.poison = {"isPoisoned":false, "damageRate":0, "turnsLeft":0};
    primus.curses = []; // curses are pushed here to lie dormant until activation
    // Perform an iteration
    DoIterateV3_( simCount++);
    // Summarize round results for histogram
    if ( primus.timesLocked[simCount-1] > 12){ // With Elsa+4, can get more than 12 locks in a single round, but lockData only premade with 0-12 as options
      try {
        Logger.log( primus.lockData[primus.timesLocked[simCount-1]].length);
      }
      catch(e){
        primus.lockData[primus.timesLocked[simCount-1]] = []; // couldn't read the length of a non-existent array, so we create it
      }
    }
    primus.lockData[primus.timesLocked[simCount-1]].push( primus.damage[simCount-1]); // histogram data of number of times locked & the damage per round
  }
  // iterations complete / timed out, report data
  doReport_( fleet, primus, fleetData, setupSS.getParent());
  if ( simParams[4][0] == true) doPlot( fleet, primus, SpreadsheetApp.getActiveSpreadsheet().insertSheet( setupSS.getParent().getSheets().length));
}

/**
 * function MakePositionMap_  Analyze the fleet global variable to determine the location of ships in the 3x3 grid
 * @return {Object}           An object with keys of "s" and the number of the position, tiled 1-2-3; 4-5-6; 7-8-9;
 *                            Values obtained are the name of the ship (e.g. the key to be used in with fleet global object)
 */
function MakePositionMap_(){
  // iterate the name-referenced fleet object to create a position-referenced object list
  var outputObj = {"s1":"", "s2":"", "s3":"", "s4":"", "s5":"", "s6":"", "s7":"", "s8":"", "s9":""};
  var insertSummon = false;
  for ( var p in fleet){
    if ( fleet[p].isDead == false){
      if ( p != "summonShip"){
        outputObj["s"+fleet[p].position.toString()]=p.toString();
      }
      else {
        insertSummon = true;
      }
    }
  }
  if ( insertSummon){             // Had to finish all other ships first
    var spot = 1;
    while ( fleet["summonShip"].position == 0){
      if ( outputObj["s"+spot.toString()] == ""){
        fleet["summonShip"].position = spot-0;
      }
      spot++;
    }
    outputObj["s"+fleet["summonShip"].position.toString()] = "summonShip";
  }
  return outputObj;
}

/**
 * function MakeAttackOrder_    Analyze the fleetMap global variable to determine the order in which ships should attack Primus.
 * @param {Array} hasFiredList  A list of all ships which have already fired this round and thus should not fire again.
 * @return {Array}              The inverse order of ships which should fire. To use Array.pop(), first use Array.reverse().
 */
function MakeAttackOrder_( hasFiredList){
  var attackingOrder = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"],
      aOut = [];
  for ( var a = 0;a<9;a++){ // Loop through 'attackingOrder' list
    var shipName = String(fleetMap[attackingOrder[a]]);
    if ( shipName.length > 0){
      if ( fleet[shipName].isDead == false){
        if ( hasFiredList.indexOf( shipName) < 0){
          /*if ( a >= hasFiredList.length){
            /* if the summon is placed in 3 by quasi from space 2, a = 2 with hasFiredList.length = 2 - we expect the summon to fire
            if the summon is placed in 2 by quasi from space 4, a = 1 with hasFiredList.length = 2 - the summon's firing turn has passed
            thad - quasi - X          thad - quasi - summon
            X  -  FS   - X    becomes     X  -  FS   - X
            X  - Flynn - Vio            X  - Flynn - Vio

            thad -   X   - X          thad - summon - X
            quas -  FS   - X    becomes    quas -  FS    - X
            X  - Flynn - Vio            X  - Flynn  - Vio
            /
            aOut.push( shipName);
          }*/ //However, as of at least 2016-03-29, the summon does not fire in the turn during which it is summoned, so above code not needed (yet)
          aOut.push( shipName);
        }
      }
    }
  }
  return aOut;
}

/**
 * function MakeTargetOrder_  Analyze the fleetMap global variable to determine the order in which ships should be targeted by Primus attacks.
 * @return {Array}            The ordered array of positions to target. The next target is obtained with Array.pop().
 */
function MakeTargetOrder_(){ //output is directly pop()-able
  var targetingOrder = ["s2", "s5", "s8", "s1", "s4", "s7", "s3", "s6", "s9"],
      tOut = [];
  for ( var a = 0;a<9;a++){ // Loop through 'targetingOrder' list
    var shipName = String(fleetMap[targetingOrder[a]]);
    if ( shipName.length > 0){
      if ( fleet[shipName].isDead == false){
        tOut.push( shipName);
      }
    }
  }
  return tOut.reverse();
}

/**
 * function TryToKillShip_      Routine for determining if the targeted ship survived or died. Updates relevant global properties as appropriate.
 * @param {String} targetedShip The name of the targeted ship in the fleet object
 * @param {Number} simNumber    The current history number being simulated
 * @param {Boolean} isCounter   True/False for if the potential death action resulted from Primus counterattacking a blocked attack
 * @param {Boolean} isSATK      True/False for if the potential death action is because Primus is activating its S-ATK skill
 * @return {Boolean}            True/False for if the targeted ship was killed.
 */
function TryToKillShip_( targetedShip, simNumber, isCounter, isSATK){
  var isDead = false;
  if ( fleet[targetedShip].isDead == true){
    isDead = true;
  }
  else {
    if ( isCounter == true){    // Primus counterattack - guaranteed hit, we cannot respond, invisibility doesn't matter
      primus.accumulator += 25; // Primus only gains 25 from successful block+counter
      if (( fleet[targetedShip].stasis.isDeathProof == false) && ( fleet[targetedShip].immunity.isImmune == false)){
        isDead = true;
      }
      else {
        if ( fleet[targetedShip].immunity.isImmune){
          // Immunity saved the ship, as it prevents all damage
          fleet[targetedShip].immunity.hitsLeft = Math.max( 0, fleet[targetedShip].immunity.hitsLeft-1);
          if ( fleet[targetedShip].immunity.hitsLeft == 0){
            fleet[targetedShip].immunity.isImmune = false;
            fleet[targetedShip].immunity.turnsLeft = 0;
          }
        }
        else {
          // Stasis saved the ship from death, as it was not immune to damage
          fleet[targetedShip].stasis.isDeathProof = false;
          fleet[targetedShip].stasis.turnsLeft = 0;
        }
        // Regardless of the saving method, gain accumulator
        fleet[targetedShip].accumulator += (targetedShip.toLowerCase() == "alfred") ? 0 : 25;
      }
    }
    else {                      // Primus ATK or S-ATK, we can perhaps respond
      if ( isSATK == true){     // Primus S-ATK reveals invisible ships
        fleet[targetedShip].invis.isInvisible = false;
        fleet[targetedShip].invis.turnsLeft = 0;
        fleet[targetedShip].invis.from = "";
      }
      if ( fleet[targetedShip].invis.isInvisible == true){
        throw new Error('Attempting to kill invisible ship without using S-ATK reveal');
        isDead = false;         // Can't kill what can't be seen
      }
      else {
        // Check for dodge
        fleet[targetedShip].dodgeRate = 100 - (primus.hitRate - GetFinalStat_( targetedShip, "dodge"));
        // Example:  alfred.dodgeRate = 100 - (155  - 80)  = 100 - (75) = 25
        if ( Math.random()*100 < fleet[targetedShip].dodgeRate){  // Primus missed!
          isDead = false;
        }
        else {
          // Successful hit
          if ( isSATK == false){
            primus.accumulator += 25;
          }
          if (( fleet[targetedShip].stasis.isDeathProof == false) && ( fleet[targetedShip].immunity.isImmune == false)){
            isDead = true;
          }
          else {
            if ( fleet[targetedShip].immunity.isImmune){
              // Immunity saved the ship, as it prevents all damage
              fleet[targetedShip].immunity.hitsLeft = Math.max( 0, fleet[targetedShip].immunity.hitsLeft-1);
              if ( fleet[targetedShip].immunity.hitsLeft == 0){
                fleet[targetedShip].immunity.isImmune = false;
                fleet[targetedShip].immunity.turnsLeft = 0;
              }
            }
            else {
              // Stasis saved the ship from death, as it was not immune to damage
              fleet[targetedShip].stasis.isDeathProof = false;
              fleet[targetedShip].stasis.turnsLeft = 0;
            }
            // Regardless of the saving method, gain accumulator
            fleet[targetedShip].accumulator += (targetedShip.toLowerCase() == "alfred") ? 0 : 25;// Check for block & counter
            if ( isSATK == false){
              var currentBlockRate = GetFinalStat_( targetedShip, "block") - primus.pen;
              if ( Math.random()*100 <= currentBlockRate){
                // Block + Counter Successful. We gain 50 accumulator
                primus.timesHit[simNumber]++;
                primus.timesCountered[simNumber]++;
                fleet[targetedShip].accumulator += 50;
                // Ensure our attributes are up-to-date
                fleet[targetedShip].ATK = GetFinalStat_( targetedShip, "atk");
                fleet[targetedShip].critChance = GetFinalStat_( targetedShip, "critChance");
                fleet[targetedShip].critDam = fleet[targetedShip].rawCritDamage-0;
                fleet[targetedShip].dmgUp = GetFinalStat_( targetedShip, "dmgUp");
                fleet[targetedShip].sDmgUp = GetFinalStat_( targetedShip, "sDmgUp");
                // Perform attack
                var damage = fleet[targetedShip].ATK*(1+controlParams.forceUp/100)*(1+fleet[targetedShip].dmgUp/100);
                // Counterattacks have variable damage output, around ~70%. Here, 65%-75% is possible
                damage = damage * (0.7 + (0.5-Math.random())/10)
                if ( Math.random()*100 <= fleet[targetedShip].critChance) damage = damage*fleet[targetedShip].critDam/100;
                damage = Math.floor(damage);
                fleet[targetedShip].damageDealt[simNumber] += damage-0;
                primus.damage[simNumber] += damage-0;
              }
            }
          }
        }
      }
    }
    if ( isDead == true){
      fleet[targetedShip].isDead = true; // mark the ship as dead
      fleet[targetedShip].accuOnDeath[simNumber] = fleet[targetedShip].accumulator; // Store how much they had left
      if ( isSATK == true) primus.satkKills[simNumber]++;
      if ( fleet[targetedShip].wainFury.isLinked == true){
        fleet[fleet[targetedShip].wainFury.from].wainFury.amount = controlParams.wain.multiplier*1;
        fleet[fleet[targetedShip].wainFury.from].wainFury.turnsLeft = 3;
      }
      // test "oops now our fleet is totally dead" condition
      controlParams.fleetAlive = false; // make assumption
      for ( var i in fleet){
        controlParams.fleetAlive = controlParams.fleetAlive || (!(fleet[i].isDead)); // Check assumption
      }
      controlParams.fleetDead = (!(controlParams.fleetAlive));
      if ( controlParams.fleetDead == false){ // this stuff needs to be done, since life goes on
        switch ( fleet[targetedShip].name){ // Remove invisibility from other ships if we gave it to them
          case "akhenaton":
            for ( var i in fleet){
              if ( fleet[i].invis.from == "akhenaton"){
                fleet[i].invis.isInvisible = false;
                fleet[i].invis.turnsLeft = 0;
                fleet[i].invis.from = "";
              }
            }
            break;
          case "izolda":
            for ( var i in fleet){
              if ( fleet[i].invis.from == "izolda"){
                fleet[i].invis.isInvisible = false;
                fleet[i].invis.turnsLeft = 0;
                fleet[i].invis.from = "";
              }
            }
            break;
          case "summonShip":
            fleet[targetedShip].turnsLeft = 0;
            fleet[targetedShip].position = 0;
            controlParams.updateLists = true;
            break;
          case "alfred": // alfred's curses are removed when he dies before they activate
            var i = 0;
            while ( i < primus.curses.length){
              if ( primus.curses[i].from == "alfred"){
                primus.curses.splice( i, 1); // remove that curse (shortening the array, so cannot use +for loop)
              }
              else {
                i++;
              }
            }
            break;
          default:
            break;
        }
      }
    }
  }
  return isDead;
}

/**
 * function GetFinalStat_  Properly combine raw attributes, bonuses from S-ATK effects, and bonuses from Lieutenants.
 *                         Note that bonuses to attributes are not multipliers (e.g. Ursa Wainfury buff is completely separate from this)
 * @param {String} ship The name of a ship in the fleet object
 * @param {String} stat The attribute which is to be (re-)calculated
 * @return {Number}     The requested attribute, including all available attribute bonuses.
 */
function GetFinalStat_( ship, stat){
  var boost = 0, rawStat = 0, finalStat = 0;
  boost = fleet[ship].boostFromSatk[stat].perm-0 + fleet[ship].boostFromSatk[stat].temp-0;   // bonuses from S-ATKs
  boost += fleet[ship].ltSpecial[stat].perm-0 + fleet[ship].ltSpecial[stat].temp-0;       // bonuses from Lt. effects
  switch ( stat){
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
      break;
    case "dmgUp":
      rawStat = fleet[ship].rawDmgUp-0;
      finalStat = rawStat-0+boost-0;
      break;
    case "sDmgUp":
      rawStat = fleet[ship].rawSDmgUp-0;
      finalStat = rawStat-0+boost-0;
      break;
    default:
      return -100000000;
      break;
  }
  return finalStat-0;
}

/**
 * function DoIterateV3_  Simulate 1 battle with Primus. Automatically quit if we "over-damage" Primus
 * @param {Integer} simNum The current history. Used to allow comparison between histories.
 */
function DoIterateV3_( simNum){
  var targetList = [],
      attackList = [],
      didAttack = [],
      currentTarget = ""
  // Initialize fleetMap
  fleetMap = MakePositionMap_();
  controlParams.fleetDead = false;
  controlParams.fleetAlive = true;
  while (( controlParams.fleetDead == false) && ( primus.damage[simNum] < 5e10)){
    if (controlParams.summonWasUsed == true){
      // Summon ship may exist, and should be removed if it has already used its turn
      if ( fleet["summonShip"].position != 0){
        if ( fleet["summonShip"].turnsLeft < 1){
          fleetMap["s"+fleet["summonShip"].position] = "";
          fleet["summonShip"].position = 0;
          fleet["summonShip"].isDead = true;
          fleet["summonShip"].accuOnDeath[simNum] = fleet["summonShip"].accumulator*1;
        /*  if ( fleet["summonShip"].wainFury.isLinked == true){
            fleet[fleet["summonShip"].wainFury.from].wainFury.amount = controlParams.wain.multiplier*1;
            fleet[fleet["summonShip"].wainFury.from].wainFury.turnsLeft = 3;
          } */
        }
      }
      // Regenerate the position map of the fleet since the summonShip can take a variable position (or vanish!)
      fleetMap = MakePositionMap_();
    }
    didAttack = []; // No one has fired yet on this turn
    attackList = MakeAttackOrder_( didAttack);
    if ( attackList.length > 0){
      attackList.reverse();
      var shipName = String(attackList.pop());
      CheckLieutenants_( shipName, true);
      DoAttackV3_( fleet[shipName], simNum);
      didAttack.push( shipName.toString());
      if ( controlParams.updateLists == true){
        // Duomilian was inserted into the fleet, possibly changing the attack order, and definitely altering the target order
        fleetMap = MakePositionMap_();
//        attackList = MakeAttackOrder_( didAttack).reverse(); // The summon ship does not get to attack on the turn in which it is summoned
        controlParams.updateLists = false;
      }
      // Primus turn - assess curses, poison, locks, firing
      if ( controlParams.fleetDead == false){
        // Curse activations
        if ( primus.curses.length > 0){
          var i = 0;
          while ( i < primus.curses.length){
            if ( primus.curses[i].dormantTurns == 0){
              primus.damage[simNum] += primus.curses[i].damageRate-0;
              fleet[primus.curses[i].from].damageDealt[simNum] += primus.curses[i].damageRate-0;
              primus.curses.splice( i, 1); // Remove the now-activated curse
            }
            else {
              primus.curses[i].dormantTurns = Math.max( 0, primus.curses[i].dormantTurns-1);
              i++;
            }
          }
        }
        // Poison Damage Application
        if ( primus.poison.isPoisoned == true){
          primus.damage[simNum] += primus.poison.damageRate-0;
          if ( primus.poison.turnsLeft <= 1){
            primus.poison.isPoisoned = false;
            primus.poison.damageRate = 0;
          }
          primus.poison.turnsLeft = Math.max( 0, primus.poison.turnsLeft-1);
        }
        // Kit+4 check (Fortress-Class Primus Invasion Primus variant)
        if ( primus.satkDisabled.turnsLeft < 1){
          primus.satkDisabled.value = 1; // 1 = not disabled, 0 = disabled
          primus.satkDisabled.turnsLeft = 0;
        }
        else {
          primus.satkDisabled.turnsLeft = Math.max( 0, primus.satkDisabled.turnsLeft-1);
        }
        // Lock Check / Clear
        if ( primus.isLocked == true){
          primus.timesLocked[simNum]++;
          primus.lockedTurns = Math.max( 0, primus.lockedTurns-1);
          if ( primus.lockedTurns == 0){
            primus.isLocked = false;
          }
        }
        else {
          primus.canBlock = true;

          // Firing opportunity is realized
          targetList = MakeTargetOrder_(); // make the target list
          if ( targetList.length > 0){
            var isPrimusSATK = (primus.accumulator * primus.satkDisabled.value >= 100) ? true : false;
            if ( isPrimusSATK){
              // S-ATK
              for ( var i in fleet){
                TryToKillShip_( i, simNum, false, isPrimusSATK);
              }
              primus.accumulator = 0;
            }
            else {
              // Regular ATK
              // Obtain non-invisible ship to try to hit
              var ship = targetList.pop();
              while ( fleet[ship].invis.isInvisible == true && targetList.length > 0){
                ship = targetList.pop();
              }
              if ( targetList.length == 0 && fleet[ship].invis.isInvisible == true){
                // Primus skips a turn since everything alive is also invisible
              }
              else {
                TryToKillShip_( ship, simNum, false, isPrimusSATK); //
              }
            }
          } // end the have-targets code
        } // end lock-check/primus-tries-to-do-damage code
      } // end of primus's turn code

      // Send the rest of the fleet
      while ( attackList.length > 0){
        var shipName = String(attackList.pop());
        CheckLieutenants_( shipName, true);
        DoAttackV3_( fleet[shipName], simNum);
        didAttack.push( shipName);
        if ( controlParams.updateLists == true){
          fleetMap = MakePositionMap_();
//          attackList = MakeAttackOrder_( fleet, didAttack).reverse(); // Summon ship doesn't get attack opportunity until next battle round
          controlParams.updateLists = false;
        }
      }
    }
    else {
      // Empty attackList before attacking --> our fleet is wholly dead
      controlParams.fleetDead = true;
      controlParams.fleetAlive = false;
    }
  }
}

/**
 * function DoAttackV3_      Target Primus with the passed ship object and perform relevant operations such as turn-based effect removal and damage calculations
 * @param {Object} attacker  A pointer reference to the ship performing the attack, from the fleet global variable.
 * @param {Integer} simNumber The current history being computed
 */
function DoAttackV3_( attacker, simNumber){
  var shotBlocked = false,    shotCountered = false,    isSatk = false,
      damage = 0,   accuDrain = 0,      dorBuff = 0,        wainFury = 0;
  if ( attacker.isDead == true) return;
  attacker.numShots[simNumber]++;

  // Remove expiring turn-based effects
  // An effect is expiring before firing if its current value of tempTurns is 0.
  for ( var i in attacker.ltSpecial){
    if ( attacker.ltSpecial[i].tempTurns == 0) attacker.ltSpecial[i].temp = 0;
    attacker.ltSpecial[i].tempTurns = Math.max( 0, attacker.ltSpecial[i].tempTurns-1)
  }
  for ( var i in attacker.boostFromSatk){
    if ( attacker.boostFromSatk[i].tempTurns == 0) attacker.boostFromSatk[i].temp = 0;
    attacker.boostFromSatk[i].tempTurns = Math.max( 0, attacker.boostFromSatk[i].tempTurns-1)
  }
  if ( attacker.stasis.turnsLeft == 0) attacker.stasis.isDeathProof = false;
  attacker.stasis.turnsLeft = Math.max( 0, attacker.stasis.turnsLeft-1);
  if ( attacker.invis.turnsLeft == 0){
    attacker.invis.isInvisible = false;
    attacker.invis.from = "";
  }
  attacker.invis.turnsLeft = Math.max( 0, attacker.invis.turnsLeft-1);
  if ( attacker.wainFury.turnsLeft == 0){
    attacker.wainFury.amount = 1;       // Will expire Ursa's multiplier buff
    attacker.wainFury.isLinked = false; // Will expire linked state
  }
  attacker.wainFury.turnsLeft = Math.max( 0, attacker.wainFury.turnsLeft-1);
  if ( attacker.immunity.turnsLeft == 0){
    attacker.immunity.isImmune = false;
    attacker.immunity.hitsLeft = 0;
  }
  attacker.immunity.turnsLeft = Math.max( 0, attacker.immunity.turnsLeft-1);
  if ( attacker.name == 'summonShip') attacker.turnsLeft = Math.max( 0, attacker.turnsLeft-1);

  // Generate current attributes
  attacker.ATK = GetFinalStat_( attacker.name, "atk");
  attacker.SATK = GetFinalStat_( attacker.name, "satk");
  attacker.pen = GetFinalStat_( attacker.name, "pen");
  attacker.hitRate = GetFinalStat_( attacker.name, "hitRate")-primus.dodgeRate;
  attacker.critChance = GetFinalStat_( attacker.name, "critChance");
  attacker.critDam = attacker.rawCritDamage-0;
  attacker.dmgUp = GetFinalStat_( attacker.name, "dmgUp");
  attacker.sDmgUp = GetFinalStat_( attacker.name, "sDmgUp");

  // Do we hit Primus?
  if ( Math.random()*100 > attacker.hitRate){
    attacker.numMisses++;
  }
  else {
    // Hit, but blocked?
    primus.timesHit[simNumber]++;
    var blockRate = (primus.blockRate - attacker.pen) * primus.canBlock;
    if ( Math.random()*100 <= blockRate){
      shotBlocked = true;
      attacker.numBlocked++;
    }

    // Compute dealt damage
    if ( attacker.accumulator > 99){
      isSatk = true;
      attacker.numSATK[simNumber]++;
      // Calculate drained accumulator / other pre-S-ATK effects
      switch ( attacker.name.toLowerCase()){
        case "sky mightlis+15":
          accuDrain = 70;
        case "sky mightlis+12":
          accuDrain = Math.max( accuDrain, 60);
        case "sky mightlis+7":
          accuDrain = Math.max( accuDrain, 50);
        case "sky mightlis+5":
          accuDrain = Math.max( accuDrain, 45);
        case "sky mightlis":
          accuDrain = Math.max( accuDrain, 40);
          break;
        case "starlord":
          accuDrain = 20;
          break;
        default:
          break;
      }
      accuDrain = Math.min( primus.accumulator, accuDrain);
      primus.accumulator = Math.max( 0, primus.accumulator - accuDrain);
      attacker.accumulator += accuDrain*1;
    }
    damage = (attacker.ATK-0 + isSatk*attacker.SATK)*(1+controlParams.forceUp/100)*(1+isSatk*(attacker.SATKDamage/100 - 1))*(1+isSatk*(attacker.accumulator-100)/100)*(1+attacker.dmgUp/100)*(1+isSatk*attacker.sDmgUp/100)*attacker.wainFury.amount;
    if ( Math.random()*100 <= attacker.critChance) damage = damage*(attacker.critDam/100);
    if ( shotBlocked) damage = damage*controlParams.blockAmt;
    damage = Math.floor( damage);
    primus.damage[simNumber] += damage-0;
    attacker.damageDealt[simNumber] += damage-0;

    // S-Attack effects
    if ( isSatk){
      DoSpecialSATK_Effects_( attacker);
      // assess Primus lock, accumulator, poison
      if ( Math.random()*100 <= attacker.lockRate){
        primus.isLocked = true;
        primus.canBlock = false;
        primus.lockedTurns = ((attacker.name.toLowerCase() == "velarath") ? 2 : 1);
      }
      if ( Math.random()*100 <= attacker.prbDeAccum){
        primus.accumulator = Math.max( 0, primus.accumulator-((attacker.deAccumulate == 100) ? 300 : attacker.deAccumulate));
      }
      else if ( attacker.name == 'starlord'){
       primus.accumulator = Math.floor( primus.accumulator*(1-0.35));
      }
      if ( attacker.poisonPercent > 0){
        primus.poison.isPoisoned = true;
        primus.poison.damageRate = attacker.poisonPercent/100*(1+controlParams.forceUp/100)*(attacker.ATK-0 + attacker.SATK-0)*(1+attacker.dmgUp/100+attacker.sDmgUp/100);
        primus.poison.turnsLeft = 2;
      }
    }
    else {
      attacker.accumulator += 25;
      // Assess counterattack possibility
      if ( shotBlocked){
        if ( attacker.isDestroyer == true || (!(primus.canBlock))){
        }
        else {
          TryToKillShip_( attacker.name, simNumber, true, false);
        }
      }
    } // end S-ATK effects codes
  } // end we-tried-to-hit-the-primus codes
}
