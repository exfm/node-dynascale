"use strict";

// Take the derivative of an array
module.exports.arrayDerivative = function(f){
	var values = [],
		i;
	for (i = 0; i < f.length; i++){
		if (i === 0){
			values.push(f(f[0] + 1) - f(0));
		}
		else if (i === (f.length - 1)){
			values.push(f(i) - f(i-1));
		}
		else {
			values.push((f(i+1) - f(i-1)) / 2);
		}
	}
	return values;
};