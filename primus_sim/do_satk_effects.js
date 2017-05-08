/**
* @OnlyCurrentDoc
*/
// written by z3nithor, world 747
/**
 * function DoSpecialSATK_Effects_  Allow extra effects/conditions to be activated other than simple damage, poison damage, or lock/freeze effects
 *                                  Operates on the global variables fleet, primus
 * @param  {Object} attacker        A pointer to the fleet[ship] object whose effects which require activating
 */
function DoSpecialSATK_Effects_( attacker){
  attacker.accumulator = attacker.SATKReset;
  switch (attacker.name.toLowerCase()){
    case "frost jr":
      // Increment a random ship's accumulator by 30 (if it is alive)
      var choices = [];
      for (var selection in fleet){
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      fleet[choices[Math.floor(Math.random()*choices.length)]].accumulator += 30;
      break;
    case "cabal":
      // Self-invisibility & 2x 20% Dodge increases
      // Cabal's iself-nvisibility lasts one round longer than Alfred's, as it takes effect after firing
      attacker.invis.isInvisible = true;
      attacker.invis.from = "cabal";
      attacker.invis.turnsLeft = 2;
      AddTempAttributeViaSATKtoRandom_( 'dodge', 20, 1, 2);
      /*var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      var nRolls = Math.min(2,choices.length);
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1)[0];
        fleet[selection].boostFromSatk.dodge.temp = 20;
        fleet[selection].boostFromSatk.dodge.tempTurns = 1;
      }*/
      break;
    case "dark carter":
      // Increase fleet's Accumulator by 35
      for (var selection in fleet){
        if (!(fleet[selection].isDead)) fleet[selection].accumulator += 35;
      }
      break;
    case "opal":
      // Set fleet's Dodge bonus to 10%
      AddTempAttributeViaSATKtoAll_( 'dodge', 10, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.dodge.temp = 10;
        fleet[selection].boostFromSatk.dodge.tempTurns = 1;
      }*/
      break;
    case "akhenaton":
      // Set all other ships invisible, and maybe give Primus a 100,000 HP shield
      if (Math.random()*100 < 10) primus.damage[simNumber] -= 100000;
      AddInvisibilityViaSATK_( 1, 5, 'akhenaton');
      /*for (var selection in fleet) {
        switch (fleet[selection].name) {
          // These ships cannot become invisible
          case "akhenaton":
          case "izolda":
          case "raksha":
            break;
          default:
            if (!(fleet[selection].isDead)) {
              fleet[selection].invis.isInvisible = true;
              fleet[selection].invis.from = "akhe";
              fleet[selection].invis.turnsLeft = 1;
            }
            break;
        }
      }*/
      break;
    case "carter":
      // Increase fleet's Accumulator by 50
      for (var selection in fleet){
        if (!(fleet[selection].isDead)) fleet[selection].accumulator += 50;
      }
      break;
    case "cryptor":
      // Give an invincibility buff to self, and the next-to-fire ship, that clears after 5 attacks
      attacker.stasis.isDeathProof = true;
      attacker.stasis.turnsLeft = 5;
      AddInvincibilityViaSATK_( 5, 1, 'cryptor');
      /*var attackerList = makeAttackOrder_( fleet, fleetMap, '');
      if (attackerList.length > 1) {
        attackerList.reverse();
        var selection = attackerList.pop();
        if (selection == "cryptor") selection = attackerList.pop();
        fleet[selection].stasis.isDeathProof = true;
        fleet[selection].stasis.turnsLeft = 5;
      }*/
      break;
    case "hunter":
      // Set two ships' temporary Hit Rate bonus to 20% for 2 attacks
      AddTempAttributeViaSATKtoRandom_( 'hitRate', 20, 2, 2);
      /*var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      var nRolls = Math.min(2,choices.length);
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1);
        fleet[selection].boostFromSatk.hitRate.temp = 20;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 2;
      }*/
      break;
    case "quasimodo":
      // Summon Duomilian to the fleet, in the first position not filled by a living ship
      controlParams.updateLists = true;
      fleet['summonShip'].isDead = false;
      fleet['summonShip'].accumulator = 0;
      // Trigger insertion mode behavior of the positionMap function
      fleet['summonShip'].position = 0;
      fleet['summonShip'].turnsLeft = 1;
      break;
    case "darrien":
      // Set fleet's temporary Hit Rate bonus to 100%, and temporary Block bonus to 100% for 3 ships, for 1 attack
      AddTempAttributeViaSATKtoRandom_( 'hitRate', 100, 1, 3);
      AddTempAttributeViaSATKtoRandom_( 'block', 100, 1, 3);
      /*var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) {
          fleet[selection].boostFromSatk.hitRate.temp = 100;
          fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
          choices.push(String(selection));
        }
      }
      var nRolls = Math.min(3,choices.length);
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1);
        fleet[selection].boostFromSatk.block.temp = 100;
        fleet[selection].boostFromSatk.block.tempTurns = 1;
      }*/
      break;
    case "dark darrien":
      // Set 3 ships' temporary Hit Rate bonus to 60%, and 3 ships' temporary Block bonus to 60%, for 1 attack
      AddTempAttributeViaSATKtoRandom_( 'hitRate', 60, 1, 3);
      AddTempAttributeViaSATKtoRandom_( 'block', 60, 1, 3);
      /*var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      var nRolls = Math.min(3,choices.length);
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1);
        fleet[selection].boostFromSatk.hitRate.temp = 60;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
      }
      var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1);
        fleet[selection].boostFromSatk.block.temp = 60;
        fleet[selection].boostFromSatk.block.tempTurns = 1;
      }*/
      break;
    case "lazarus":
      // Set fleet's temporary S-ATK bonus to 30% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'satk', 0.30, 1);
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.satk.temp = 0.3;
        fleet[selection].boostFromSatk.satk.tempTurns = 1;
      }*/
      break;
    case "dark lazarus":
      // Set fleet's temporary S-ATK bonus to 25% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'satk' 0.25, 1);
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.satk.temp = 0.25;
        fleet[selection].boostFromSatk.satk.tempTurns = 1;
      }*/
      break;
    case "roxy":
      // Set fleet's temporary Crit and Hit Rate bonuses to 25% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'critChance', 25, 1, '');
      AddTempAttributeViaSATKtoAll_( 'hitRate', 25, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.hitRate.temp = 25;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
        fleet[selection].boostFromSatk.critChance.temp = 25;
        fleet[selection].boostFromSatk.critChance.tempTurns = 1;
      }*/
      break;
    case "kit":
      // Set attacker's temporary Block bonusself block shield
      attacker.boostFromSatk.block.temp = 30
      attacker.boostFromSatk.block.tempTurns = 2;
      break;
    case "warden":
      // Set fleet's temporary Block bonus to 30% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'block', 30, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.block.temp = 30;
        fleet[selection].boostFromSatk.block.tempTurns = 1;
      }*/
      break;
    case "izolda":
      AddInvisibilityViaSATK_( 1, 1, 'izolda');
      /*izoldaInvisCheck:
      var attackerList = makeAttackOrder_( fleet, fleetMap, '');
      if (attackerList.length > 1) {
        attackerList.reverse();
        var selection = attackerList.pop();
        if (selection == "izolda") {
          if (attackerList.length == 0) break izoldaInvisCheck;
          selection = attackerList.pop();
        }
        if (selection != "akhenaton" && selection != 'raksha') {
          fleet[selection].invis.isInvisible = true;
          fleet[selection].invis.turnsLeft = 1;
          fleet[selection].invis.from = "izolda";
        }
      }*/
      break;
    case "dark kerom":
    case "kerom":
      // Set fleet's temporary Hit Rate bonus to 20% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'hitRate', 20, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.hitRate.temp = 20;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
      }*/
      break;
    case "celeste":
      // Set fleet's temporary Hit Rate and Crit bonus to 100% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'hitRate', 100, 1, '');
      AddTempAttributeViaSATKtoAll_( 'critChance', 100, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.hitRate.temp = 100;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
        fleet[selection].boostFromSatk.critChance.temp = 100;
        fleet[selection].boostFromSatk.critChance.tempTurns = 1;
      }*/
      break;
    case "dark celeste":
      // Set fleet's temporary Hit Rate and Crit bonus to 50% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'hitRate', 50, 1, '');
      AddTempAttributeViaSATKtoAll_( 'critChance', 50, 1, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.hitRate.temp = 50;
        fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
        fleet[selection].boostFromSatk.critChance.temp = 50;
        fleet[selection].boostFromSatk.critChance.tempTurns = 1;
      }*/
      break;
    case "gambit":
      // Set fleet's temporary ATK attribute bonus to 20% for 2 attacks
      AddTempAttributeViaSATKtoAll_( 'atk', 0.2, 2, '');
      /*for (var selection in fleet) {
        fleet[selection].boostFromSatk.atk.temp = 0.2;
        fleet[selection].boostFromSatk.atk.tempTurns = 2;
      }*/
      break;
    case "alfred":
      // Set self invisibility, and instantiate a curse object on Primus.
      // Curse objects are based on the attributes at the time of instantiation, and not on the attributes at the time of damage application
      attacker.invis.isInvisible = true;
      attacker.invis.turnsLeft = 1;
      attacker.invis.from = "alfred";
      primus.curses.push({"from":"alfred",
                          "damageRate": 4*(1+controlParams.forceUp/100)*(attacker.ATK-0+attacker.SATK-0)*(1+attacker.dmgUp/100+attacker.sDmgUp/100),
                          "dormantTurns":1
                         });
      break;
    case "raizer":
      // Increase fleet's Accumulator by 75
      for (var selection in fleet){
        if (fleet[selection].isDead == false) fleet[selection].accumulator += 75;
      }
      break;
    case "ursa":
      attacker.boostFromSatk.block.temp = 80;
      attacker.boostFromSatk.block.tempTurns = 4; // might be 3, might be permanent, but 4 is basically permanent
      for (var selection in fleet){
        if (fleet[selection].isDead == false ){
          fleet[selection].wainFury.isLinked = true;
          fleet[selection].wainFury.turnsLeft = 99;
          fleet[selection].wainFury.from = attacker.name.toLowerCase();
        }
      }
      controlParams.wain = {"multiplier":8*1};
      break;
    case "dor+10":                    // 80% boost
      dorBuff = 80;
    case "dor+7":                     // 65% boost
      dorBuff = Math.max(dorBuff,65);
    case "dor":                       // 50% boost
      dorBuff = Math.max(dorBuff,50);
      AddTempAttributeViaSATKtoAll_( 'hitRate', dorBuff, 1, '');
      AddTempAttributeViaSATKtoAll_( 'critChance', dorBuff, 1, '');
      /*for (var selection in fleet) {
        if (fleet[selection].isDead == false ) {
          fleet[selection].boostFromSatk.hitRate.temp = dorBuff;
          fleet[selection].boostFromSatk.hitRate.tempTurns = 1;
          fleet[selection].boostFromSatk.critChance.temp = dorBuff;
          fleet[selection].boostFromSatk.critChance.tempTurns = 1;
        }
      }*/
      dorBuff = 0;
      break;
    case "paccar":
      // Set fleet's temporary S-ATK attribute bonus to 30% for 1 attack
      AddTempAttributeViaSATKtoAll_( 'satk', 0.3, 1, '');
      /*for (var selection in fleet) {
        if (fleet[selection].isDead == false) {
          fleet[selection].boostFromSatk.satk.temp = 0.3;
          fleet[selection].boostFromSatk.satk.tempTurns = 1;
        }
      }*/
      break;
    case "anatoli":
      // Set 3 ships' temporary Crit bonuses to 100% for 1 attack
      AddTempAttributeViaSATKtoRandom_( 'critChance', 100, 1, 3);
      /*var choices = [];
      for (var selection in fleet) {
        if (!(fleet[selection].isDead)) choices.push(String(selection));
      }
      var nRolls = Math.min(3,choices.length);
      for (var i=0;i<nRolls;i++) {
        var rndRoll = Math.floor(Math.random()*choices.length);
        var selection = choices.splice(rndRoll,1);
        fleet[selection].boostFromSatk.critChance.temp = 100;
        fleet[selection].boostFromSatk.critChance.tempTurns = 1;
      }*/
      break;
    case "st. nick":
      attacker.boostFromSatk.critChance.temp = 100;
      attacker.boostFromSatk.critChance.tempTurns = 1;
      break;
    case "louise":
      // Increase fleets' Accumulator by 25 & give invincibility for 1 attack to the whole fleet
      AddInvincibilityViaSATK_( 1, 9, '');  // set origin as '' so louise is shielded too
      for (var selection in fleet){
        if (!(fleet[selection].isDead)){
          fleet[selection].accumulator += 25;
        }
      }
      // Reset Louise's accumulator back to 50
      attacker.accumulator = 50;
      break;
    case "caroline":
      // Increase own dodge and dodge of next-to-fire by 80%
      // Accumulator reset should be handled by player's inputs on the spreadsheet
      attacker.boostFromSatk.dodge.temp = 80;
      attacker.boostFromSatk.dodge.tempTurns = 1;
      AddTempAttributeViaSATKtoNext_( "dodge", 80, 1, 1, "caroline");
    default:
      break;
  }
}

/**
 * function AddInvincibilityViaSATK_  Sets the supplied number of ships to be invincible, beginning with the first-to-fire.
 * @param {Integer} duration   How many turns the invincibility buff should last
 * @param {Integer} numberToDo How many ships the invincibility buff shoudl be given to
 * @param {String} origin     The name of the casting ship
 */
function AddInvincibilityViaSATK_( duration, numberToDo, origin){
  numberToDo = numberToDo||9;
  origin = String( origin)||"";
  numberToDo = Math.min( numberToDo, Object.keys( fleet).length);
  // Get all non-dead ships in the fleet
  var attackerList = MakeAttackOrder_( []]);
  if (attackerList.length > 1){
    if (numberToDo < attackerList.length){
      // Give invincibility shields to the next N ships in the firing order, excluding the ship referenced by origin
      attackerList.reverse();
      for (var i=0;i<numberToDo;i++){
        var selection = attackerList.pop();
        if (selection == origin) selection = attackerList.pop();
        fleet[selection].stasis.isDeathProof = true;
        fleet[selection].stasis.turnsLeft = duration*1;
      }
    }
    else {
      // We are giving out as many or more invincibility shields as there are ships alive
      for (var selection in attackerList){
        fleet[selection].stasis.isDeathProof = true;
        fleet[selection].stasis.turnsLeft = duration*1;
      }
    }
  }
}

/**
 * function AddInvisibilityViaSATK_  Sets the supplied number of ships to be invisible, beginning with the first-to-fire. Always ignores the caster.
 * @param {Integer} duration   How many turns the invisibility should last
 * @param {Integer} numberToDo How many ships the invisibility should be given to
 * @param {String} origin     The name of the casting ship
 */
function AddInvisibilityViaSATK_( duration, numberToDo, origin){
  numberToDo = numberToDo||9;
  origin = String( origin)||"";
  var cannotHide = [ 'akhenaton', 'izolda', 'raksha', origin];
  numberToDo = Math.min( numberToDo, Object.keys( fleet).length);
  // Get all non-dead ships in the fleet
  var attackerList = MakeAttackOrder_( []);
  if (attackerList.length > 1){
    if (numberToDo < attackerList.length){
      // Give invisibility to the next N ships in the firing order, excluding the caster
      attackerList.reverse();
      for (var i=0;i<numberToDo;i++){
        var selection = attackerList.pop();
        if (cannotHide.indexOf( selection) > 0) selection = attackerList.pop();
        fleet[selection].invis.isInvisible = true;
        fleet[selection].invis.turnsLeft = duration*1;
        fleet[selection].invis.from = origin;
      }
    }
    else {
      for (var selection in attackerList){
        if (cannotHide.indexOf( selection) === -1){
          fleet[selection].invis.isInvisible = true;
          fleet[selection].invis.turnsLeft = duration*1;
          fleet[selection].invis.from = origin;
        }
      }
    }
  }
}

/**
 * function AddTempAttributeViaSATKtoAll_  Adds the specified amount to the specified attribute for the specified duration for all ships with the specified shipClass
 *                                         Temporary bonuses always overwrite other bonuses to that same attribute that are added in the same manner (e.g. from activated Lieutenants)
 *                                         Bonuses are expired before the attack is made, if the remaining duration is 0 at that moment
 * @param {String} attributeName      The attribute which is to be temporarily modified
 * @param {Number} amount             The magnitude of the temporary bonus.
 * @param {Integer} duration          The number of attacks which can be made before this bonus expires.
 * @param {String} shipClass          The type of ship which should benefit from this bonus. E.g. "", Hero, Ranger, Rover, Protector, Destroyer, or Striker
 */
function AddTempAttributeViaSATKtoAll_( attributeName, amount, duration, shipClass){
  attributeName = String( attributeName).toLowerCase();
  shipClass = String( shipClass).toLowerCase()||"";
  for (var i in fleet){
    if (fleet[i].isDead === false){
      if (shipClass === "" || shipClass === fleet[i].class.toLowerCase()){
        fleet[i].boostFromSatk[attributeName].temp = amount*1;
        fleet[i].boostFromSatk[attributeName].tempTurns = duration*1;
      }
    }
  }
}

/**
 * function AddTempAttributeViaSATKtoRandom_    Adds the specified amount to the specified attribute for up to numRecipients ships, chosen randomly from non-dead ships.
 *                                              Temporary bonuses always overwrite other bonuses to that same attribute that were added in the same manner.
 * @param {String} attributeName  The attribute to be temporarily modified
 * @param {Number} amount         The magnitude of the temporary bonus
 * @param {Integer} duration      The number of attacks which can be made before this buff expires
 * @param {Integer} numRecipients The maximum number of ships which can recieve this buff
 */
function AddTempAttributeViaSATKtoRandom_( attributeName, amount, duration, numRecipients){
  attributeName = String( attributeName).toLowerCase();
  var choices = []
  for (var selection in fleet){
    if (fleet[selection].isDead === false){
      choices.push(String( selection));
    }
  }
  var nRolls = Math.min( numRecipients*1, choices.length);
  for (var i=0;i<nRolls;i++){
    var selection = choices.splice( Math.floor( Math.random()*choices.length), 1);
    fleet[selection].boostFromSatk[attributeName].temp = amount*1;
    fleet[selection].boostFromSatk[attributeName].tempTurns = duration*1;
  }
}

/**
 * function AddTempAttributeViaSATKtoNext_    Adds the specified amount to the specified attribute for up to numRecipients ships, chosen by firing order.
 *                                            Temporary bonuses always overwrite other bonuses to that same attribute that were added in the same manner.
 * @param {String} attributeName  The attribute to be temporarily modified
 * @param {Number} amount         The magnitude of the temporary bonus
 * @param {Integer} duration      The number of attacks which can be made before this buff expires
 * @param {Integer} numRecipients The maximum number of ships which can recieve this buff
 * @param {String} origin         The ship which cast this buff, if the casting ship should not receive it
 */
function AddTempAttributeViaSATKtoNext_( attributeName, amount, duration, numRecipients, origin){
  attributeName = String( attributeName).toLowerCase();
  origin = String( origin)||"";
  // Get all non-dead ships in the fleet
  var attackerList = MakeAttackOrder_( []]);
  if (attackerList.length > 1){
    if (numRecipients < attackerList.length){
      // Give the attribute to the next N ships in the firing order, excluding the ship referenced by origin
      attackerList.reverse();
      for (var i=0;i<numRecipients;i++){
        var selection = attackerList.pop();
        if (selection == origin) selection = attackerList.pop();
        fleet[selection].boostFromSatk[attributeName].temp = amount*1;
        fleet[selection].boostFromSatk[attributeName].tempTurns = duration*1;
      }
    }
    else {
      // We are giving out as many or more attribute boosts as there are ships alive
      for (var selection in attackerList){
        fleet[selection].boostFromSatk[attributeName].temp = amount*1;
        fleet[selection].boostFromSatk[attributeName].tempTurns = duration*1;
      }
    }
  }
}

/**
 * function AddPermAttributeViaSATKtoAll_  Adds the specified amount to the specified attribute for the entire simulation for all ships with the specified shipClass
 *                                         Permanent bonuses are additive.
 * @param {String} attributeName   The attribute which is to be permanently modified
 * @param {number} amount          The magnitude of the permanent bonus.
 * @param {String} shipClass       The type of ship which should benefit from this bonus. E.g. "", Hero, Ranger, Rover, Protector, Destroyer, or Striker
 */
function AddPermAttributeViaSATKtoAll_( attributeName, amount, shipClass){
  attributeName = String( attributeName).toLowerCase();
  shipClass = String( shipClass).toLowerCase()||"";
  for (var i in fleet){
    if (fleet[i].isDead === false){
      if (shipClass === "" || shipClass === fleet[i].class.toLowerCase()){
        fleet[i].boostFromSatk[attributeName].perm += amount*1;
      }
    }
  }
}
