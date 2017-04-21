/**
 * @OnlyCurrentDoc
 */
// written by z3nithor, world 747
/**
 * function StartLieutenants_  Checks each member of the global 'fleet' object for a passive lieutenant ability.
 *                             Called at the start of every situation after the fleet object is reinitialized.
 */
function StartLieutenants_(){
  if (Object.keys(fleet).length === 0) throw new Error('Undefined Fleet');
  // activate passive lieutenant buffs
  for (var i in fleet){
    CheckLieutenants_(i,false);
  }
}
/**
 * function CheckLieutenants_  Determine if the currently active ship has a lieutenant skill that should be activated, and activate it if the required conditions are met.
 *                             Values are written to the relevant members of the 'fleet' object, in the 'ltSpecial' property of fleet's primary keys
 * @param  {String} ship       A key value for the fleet object.
 * @param  {Boolean} isTurn    true/false for determining if we should attempt passive ability activation or abilities that trigger on the ship's turn to fire
 */
function CheckLieutenants_( ship, isTurn){
  if (Object.keys(fleet).length === 0) throw new Error('Undefined Fleet');
  // Passive lt ability activation.... ltSpecial.(value).perm= ###  ltSpecial:{"atk":{"perm":0,"temp":0,"tempTurns":0}
  if (fleet[ship].isDead == true) return;
  if (isTurn == false) {
    switch ( fleet[ship].lt.toLowerCase()) {
      case "elsa+4":                        // Add 2% S-ATK attribute to all ships
      case "elsa":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.satk.perm += 0.02;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "flynn+3":                        // Add 3% Penetration to all Rangers
      case "flynn":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase() == "ranger") {
              fleet[i].ltSpecial.pen.perm += 3;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "dingo+4":                        // Add 30% to flagship S-ATK attribute
        if ( fleet[ship].ltCanActivate ) {
          fleet[ship].ltSpecial.satk.perm += 0.3;
        }
      case "dingo":                          // Add 5% Dodge to flagship
        if ( fleet[ship].ltCanActivate ) {
          fleet[ship].ltSpecial.dodge.perm += 5;
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "violette+4":                     // Add 3% Hit Rate to all ships
      case "violette":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.hitRate.perm += 3;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "pelebot":                        // Add 1% ATK to all ships
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.atk.perm += 0.01;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "simon":                          // Add 0.5% Crit to all Strikers
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="striker") {
              fleet[i].ltSpecial.critChance.perm += 0.5;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "trickster":                      // Add 3% Crit to all Destroyers
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="destroyer") {
              fleet[i].ltSpecial.critChance.perm += 3;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "e-dudo+4":                       // Add 3% Hit Rate to all Destroyers
      case "e-dudo":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="destroyer") {
              fleet[i].ltSpecial.hitRate.perm += 3;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "jackie+1":                       // Add 1% Penetration to all ships
      case "jackie":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.pen.perm += 1;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "scarlet":                        // Add 0.5% Crit to all Destroyers
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="destroyer") {
              fleet[i].ltSpecial.critChance.perm += 0.5;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "khala+4":                        // Add 2% penetration to all ships
      case "khala":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.pen.perm += 2;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "volkof+3":                       // Add 1% Crit to all Strikers
      case "volkof":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="striker") {
              fleet[i].ltSpecial.critChance.perm += 1;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "mileena+6":                      // Add 15% S-ATK to flagship
      case "mileena+5":
        if ( fleet[ship].ltCanActivate ) {
          fleet[ship].ltSpecial.satk.perm += 0.15;
        }
      case "mileena+4":                      // (Also) Add 15% ATK to flagship
        if ( fleet[ship].ltCanActivate ) {
          fleet[ship].ltSpecial.atk.perm += 0.15;
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "duomilian":                      // Add 0.5% Dodge to all ships
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.dodge.perm += 0.5;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "kilian":                         // Add 1% Dodge to all ships
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.dodge.perm += 1;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "kit+4":                          // Add 3% Block to all ships
      case "kit":
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.block.perm += 3;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "acctan":                         // Add 2% Block to all ships
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.block.perm += 2;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "e-lyon":                         // Add 1% Block to all Protectors
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            if (fleet[i].class.toLowerCase()=="protector") {
              fleet[i].ltSpecial.block.perm += 1;
            }
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      case "b-queen":                         // Add 0.5% Block to all ships
        if ( fleet[ship].ltCanActivate ) {
          for (var i in fleet) {
            fleet[i].ltSpecial.block.perm += 0.5;
          }
          fleet[ship].ltCanActivate = false;
        }
        break;
      default: break;
    }
  }
  else {
    // Turn-based lieutenant abilities
    switch (fleet[ship].lt.toLowerCase()) {
      case "flynn+3":        // If >1 alive Rangers, then 50% chance to increase Rangers' ATK attribute by 20% for 1 attack
        var rangers = [];
        for (var i in fleet) {
          if (fleet[i].class.toLowerCase()=="ranger") {
            if (fleet[i].isDead==false) {
              rangers.push(i.toString());
            }
          }
        }
        if (rangers.length > 1) {
          if (Math.random() < 0.5){
            for (var i in rangers){
              fleet[rangers[i]].ltSpecial.atk.temp = 0.2;
              fleet[rangers[i]].ltSpecial.atk.tempTurns = 1;
            }
            fleet[ship].ltSpecial.atk.tempTurns = 1;
          }
        }
        break;
      case "elsa+4":          // 50% chance to set all Rangers invisible for 1 attack
        if (fleet[ship].accumulator <= 50) {
          if (Math.random() < 0.5) {
            for (var i in fleet){
              if (fleet[i].class.toLowerCase()=="ranger") {
                fleet[i].invis.isInvisible = true;
                fleet[i].invis.turnsLeft = 0;
                fleet[i].invis.from = "elsa";
              }
            }
            // If set to 0, ship is immediately targetable because it is the one firing, with effectively no invisibility effect.
            // If set to 1, reproduces in-game behavior as of 2016-03-13
            fleet[ship].invis.turnsLeft=1;
          }
        }
        break;
      case "e-dudo+4":        // If >1 alive Destroyer, then 50% chance to increase their (E-)ATK by 20% for 1 attack
        var destroyers = [];
        for (var i in fleet) {
          if (fleet[i].class.toLowerCase()=="destroyer") {
            if (fleet[i].isDead==false) {
              destroyers.push(i.toString());
            }
          }
        }
        if (destroyers.length > 1) {
          if (Math.random() < 0.5){
            for (var i in destroyers){
              fleet[destroyers[i]].ltSpecial.atk.temp = 0.2;
              fleet[destroyers[i]].ltSpecial.atk.tempTurns = 1;
            }
            fleet[ship].ltSpecial.atk.tempTurns = 1;
          }
        }
        break;
      case "d-gonzalo":          // Add 5% ATK to all Rangers (33% if Accum <= 50, once)
        if ( fleet[ship].ltCanActivate ) {
          if (fleet[ship].accumulator <= 50) {
            if (Math.random() < 0.33) {
              for (var i in fleet) {
                if (fleet[i].class.toLowerCase()=="ranger") {
                  fleet[i].ltSpecial.atk.perm += 0.05;
                }
              }
              fleet[ship].ltCanActivate = false;
            }
          }
        }
        break;
      case "sophietia":          // Add 5% ATK to all Strikers (33% if Accum <= 50, once)
        if ( fleet[ship].ltCanActivate ) {
          if (fleet[ship].accumulator <= 50) {
            if (Math.random() < 0.33) {
              for (var i in fleet) {
                if (fleet[i].class.toLowerCase()=="striker") {
                  fleet[i].ltSpecial.atk.perm += 0.05;
                }
              }
              fleet[ship].ltCanActivate = false;
            }
          }
        }
        break;
      case "raikkonen":          // Add 5% S-ATK to all Protectors (33% if accum <= 50, once)
        if ( fleet[ship].ltCanActivate ) {
          if (fleet[ship].accumulator <= 50) {
            if (Math.random() < 0.33) {
              for (var i in fleet) {
                if (fleet[i].class.toLowerCase()=="protector") {
                  fleet[i].ltSpecial.satk.perm += 0.05;
                }
              }
              fleet[ship].ltCanActivate = false;
            }
          }
        }
        break;
      case "andre":          // Add 1% Crit to all Rovers (33% if accum > 50, once). (In-game description is incorrect)
        if ( fleet[ship].ltCanActivate ) {
          if (fleet[ship].accumulator > 50) {
            if (Math.random() < 0.33) {
              for (var i in fleet) {
                if (fleet[i].class.toLowerCase()=="rover") {
                  fleet[i].ltSpecial.critChance.perm += 1;
                }
              }
              fleet[ship].ltCanActivate = false;
            }
          }
        }
        break;
      case "dudo":          // Add 5% (E-)ATK to all Destroyers (33% if accum <= 50, once)
        if ( fleet[ship].ltCanActivate ) {
          if (fleet[ship].accumulator <= 50) {
            if (Math.random() < 0.33) {
              for (var i in fleet) {
                if (fleet[i].class.toLowerCase()=="destroyer") {
                  fleet[i].ltSpecial.atk.perm += 0.05;
                }
              }
              fleet[ship].ltCanActivate = false;
            }
          }
        }
        break;
      default:
        break;
    }
  }
}

/**
 * function AddTempAttribute_    Adds the specified amount to the specified attribute for the specified duration.
 *                               Temporary bonuses always overwrite other bonuses to that same attribute that are added in the same manner (e.g. from activated Lieutenants)
 *                               Bonuses are expired before the attack is made, if the remaining duration is 0 at that moment
 * @param {String} attributeName The attribute which is to be temporarily modified
 * @param {Number} amount        The magnitude of the temporary bonus.
 * @param {Integer} duration     The number of attacks which can be made before this bonus expires.
 * @param {String} shipClass     The type of ship which should benefit from this bonus. E.g. "", Hero, Ranger, Rover, Protector, Destroyer, or Striker
 */
function AddTempAttribute_( attributeName, amount, duration, shipClass){
  attributeName = String(attributeName).toLowerCase();
  shipClass = String(shipClass).toLowerCase();
  for (var i in fleet){
    if (shipClass === "" || shipClass === fleet[i].class.toLowerCase()){
      fleet[i].ltSpecial[attributeName].temp = amount*1;
      fleet[i].ltSpecial[attributeName].tempTurns = duration*1;
    }
  }
}

/**
 * function AddPermAttribute_    Adds the specified amount to the specified attribute for the entire simulation.
 *                               Permanent bonuses are additive.
 * @param {String} attributeName The attribute which is to be permanently modified
 * @param {number} amount        The magnitude of the permanent bonus.
 * @param {String} shipClass     The type of ship which should benefit from this bonus. E.g. "", Hero, Ranger, Rover, Protector, Destroyer, or Striker
 */
function AddPermAttribute_( attributeName, amount, shipClass){
  attributeName = String(attributeName).toLowerCase();
  shipClass = String(shipClass).toLowerCase();
  for (var i in fleet){
    if (shipClass === "" || shipClass === fleet[i].class.toLowerCase()){
      fleet[i].ltSpecial[attributeName].perm += amount*1;
    }
  }
}
