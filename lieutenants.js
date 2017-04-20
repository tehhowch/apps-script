function ltStartup_(fleet){
  // activate passive lieutenant buffs
  for (var i in fleet){
    ltCheck_(fleet,i,false);
  }
}
function ltCheck_(fleet,ship,isTurn){
  // Passive lt ability activation.... ltSpecial.(value).perm= ###  ltSpecial:{"atk":{"perm":0,"temp":0,"tempTurns":0}
  if (fleet[ship].isDead == true) return;
  if (isTurn == false) {
    switch ( fleet[ship].lt.toLowerCase()) {
      case "elsa": case "elsa+4":           // add 2% S-ATK attribute to all ships
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.satk.perm += 0.02;
			}
			fleet[ship].ltCanActivate = false;
		} // only activate once
        break;
      case "flynn": case "flynn+3":          // Add 3% penetration to all rangers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase() == "ranger") {
					fleet[i].ltSpecial.pen.perm += 3;
				}
			} 
			fleet[ship].ltCanActivate = false;
		} 
        break;
      case "dingo+4":                        // Add 30% to flagship s-atk attribute
        if ( fleet[ship].ltCanActivate ) {
			fleet[ship].ltSpecial.satk.perm += 0.3;
		}
      case "dingo":							 // Add 5% dodge to flagship
		if ( fleet[ship].ltCanActivate ) {
			fleet[ship].ltSpecial.dodge.perm += 5; 
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "violette":                       // add 3% hit rate to all ships
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.hitRate.perm += 3;
			} 
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "pelebot":                        // add 1% ATK to all ships
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.atk.perm += 0.01;
			} 
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "simon":                          // add 0.5% crit chance to all strikers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="striker") {
					fleet[i].ltSpecial.critChance.perm += 0.5;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "trickster":                      // Add 3% crit chance to all destroyers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="destroyer") {
					fleet[i].ltSpecial.critChance.perm += 3;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "e-dudo": case "e-dudo+4":        // Add 3% hit rate to all destroyers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="destroyer") {
					fleet[i].ltSpecial.hitRate.perm += 3;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "jackie": case "jackie+1":         // add 1% penetration to all ships
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.pen.perm += 1;
			} 
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "scarlet":                        // add 0.5% crit to all destroyers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="destroyer") {
					fleet[i].ltSpecial.critChance.perm += 0.5;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "khala": case "khala+4":			 // add 2% penetration to all ships
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.pen.perm += 2;
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
      case "volkof": case "volkof+3":        // add 1.0% crit to all strikers
        if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="striker") {
					fleet[i].ltSpecial.critChance.perm += 1;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
        break;
	  case "mileena+5": case "mileena+6":
		if ( fleet[ship].ltCanActivate ) {
			fleet[ship].ltSpecial.satk.perm += 0.15;
		}
	  case "mileena+4": // +6 and +5 versions flow down to this automatically
		if ( fleet[ship].ltCanActivate ) {
			fleet[ship].ltSpecial.atk.perm += 0.15;
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "duomilian":                      // add 0.5% dodge to all commanders
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.dodge.perm += 0.5;
			} 
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "kilian":                         // add 1% dodge to all commanders
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.dodge.perm += 1;
			}
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "kit": case "kit+4":				 // add 3% block to all commanders
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.block.perm += 3;
			}
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "acctan":						 // add 2% block to all commanders
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.block.perm += 2;
			}
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "e-lyon":						 // add 1% block to all protectors
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				if (fleet[i].class.toLowerCase()=="protector") {
					fleet[i].ltSpecial.block.perm += 1;
				}
			}
			fleet[ship].ltCanActivate = false;
		}
		break;
	  case "b-queen":						 // add 0.5% block to all commanders
	    if ( fleet[ship].ltCanActivate ) {
			for (var i in fleet) {
				fleet[i].ltSpecial.block.perm += 0.5;
			}
			fleet[ship].ltCanActivate = false;
		}
		break;
      default: break; 
    }
  } else {
    // turn-based lieutenants - a ship is getting ready to fire
    switch ( fleet[ship].lt.toLowerCase()) {
      case "flynn+3":        // if 2 or more alive rangers, then 50% chance to increase ATK by 20% for 1 round (may be removed after activator fires second time, )
        var Rangers = []; 
		for (var i in fleet) {
			if (fleet[i].class.toLowerCase()=="ranger") {
				if (fleet[i].isDead==false) {
					Rangers.push(i.toString());
				}
			}
		}
        if (Rangers.length>1) {
			if (Math.random()<0.5){
				for (var i in Rangers){
					fleet[i].ltSpecial.atk.temp = 0.2;
					fleet[i].ltSpecial.atk.tempTurns = 1;
				}
				fleet[ship].ltSpecial.atk.tempTurns = 1;
			}
		}
        break;
      case "elsa+4":          // 50% chance to set all rangers invisible for 1 complete turn
        if ( fleet[ship].accumulator <=50 ) {
			if (Math.random() < 0.5) {
				for (var i in fleet){
					if (fleet[i].class.toLowerCase()=="ranger") {
						fleet[i].invis.isInvisible = true;
						fleet[i].invis.turnsLeft = 0;
						fleet[i].invis.from = "elsa";
					}
				}
				fleet[ship].invis.turnsLeft=1; // Will be decremented in the attack code, which follows this
			}
		}
        break;
      case "e-dudo+4":        // count destroyers, if >=2 then 50% chance to increase ATK by 20% for 1 round
        var Destroyers = []; 
		for (var i in fleet) {
			if (fleet[i].class.toLowerCase()=="destroyer") {
				if (fleet[i].isDead==false) {
					Destroyers.push(i.toString());
				}
			}
		}
        if (Destroyers.length>1) {
			if (Math.random()<0.5){
				for (var i in Destroyers){
					fleet[i].ltSpecial.atk.temp = 0.2; 
					fleet[i].ltSpecial.atk.tempTurns = 1;
				}
				fleet[ship].ltSpecial.atk.tempTurns = 1;
			}
		}
        break;
      case "d-gonzalo":          // Add 5% ATK to all rangers (33% if accum <=50)
        if ( fleet[ship].ltCanActivate ) {
			if (fleet[ship].accumulator <=50) {
				if (Math.random()<0.33) {
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
      case "sophietia":          // add 5% ATK to all strikers (33% if accum <=50)
        if ( fleet[ship].ltCanActivate ) {
			if (fleet[ship].accumulator <=50) {
				if (Math.random()<0.33) {
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
      case "raikkonen":          // add 5% S-ATK to all protectors (33% if accum <=50)
        if ( fleet[ship].ltCanActivate ) {
			if (fleet[ship].accumulator <=50) {
				if (Math.random()<0.33) {
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
      case "andre":          // add 1% crit chance to all Rovers (33% if accum > 50
        if ( fleet[ship].ltCanActivate ) {
			if (fleet[ship].accumulator <=50) {
				if (Math.random()<0.33) {
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
      case "dudo":          // add 5% atk to all destroyers (33% if accum <=50)
        if ( fleet[ship].ltCanActivate ) {
			if (fleet[ship].accumulator <=50) {
				if (Math.random()<0.33) {
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
