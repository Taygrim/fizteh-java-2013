package ru.fizteh.fivt.students.drozdowsky;

import java.math.BigInteger;

public class CalculatorMain {
    private enum OperationType {
        MULTIPLICATION_OR_DIVISION,
        ADDITION_OR_SUBTRACTION,
        NOT_AN_OPERATION
    }
    
    private static final String BAD_EXPRESSION = "Not valid Expression";
    private static final int BASE = 19;
    private static final char BINARY_MINUS_REPLACEMENT = '~';
    
    private static void printNotValidExpressionError() {
        System.out.println(BAD_EXPRESSION);
        System.exit(1);
    }
    
    private static BigInteger modifyValueWith(BigInteger value, char sign, String modifierAsString) {
        BigInteger modifierAsInteger = new BigInteger("0");
        
        try {
            modifierAsInteger = new BigInteger(modifierAsString, BASE);
        } catch (NumberFormatException e) {
            printNotValidExpressionError();
        }
        
        switch (sign) {
            case '*':
                value = value.multiply(modifierAsInteger);
                break;
            case '/':
                if (modifierAsInteger.toString().equals("0")) {
                    printNotValidExpressionError();
                }
                value = value.divide(modifierAsInteger);
                break;
            case '+':
                value = value.add(modifierAsInteger);
                break;
            case BINARY_MINUS_REPLACEMENT:
                value = value.subtract(modifierAsInteger);
                break;
            default:
                value = modifierAsInteger;
                break;
        }
        return value;
    }
    
    private static OperationType findOperationType(char charToCheck) {
        switch (charToCheck) {
            case '*':
                return OperationType.MULTIPLICATION_OR_DIVISION;
            case '/':
                return OperationType.MULTIPLICATION_OR_DIVISION;
            case BINARY_MINUS_REPLACEMENT:
                return OperationType.ADDITION_OR_SUBTRACTION;
            case '+':
                return OperationType.ADDITION_OR_SUBTRACTION;
            default:
                return OperationType.NOT_AN_OPERATION;
        }
    }
    
    private static String computeExpressionWithOperationType(String expression, OperationType operationType) {
        StringBuilder lastNumber = new StringBuilder(expression.length());
        char lastSign = 0;
        BigInteger currentValueOfExpression = new BigInteger("0");
        boolean ableToAppend = true;
        
        for (int currentPositionInExpression = 0;
                currentPositionInExpression < expression.length();
                currentPositionInExpression++) {
            char currentLetterInExpression = expression.charAt(currentPositionInExpression);
            if (findOperationType(currentLetterInExpression) == operationType) {
                currentValueOfExpression = modifyValueWith(currentValueOfExpression, lastSign, lastNumber.toString());
                lastSign = expression.charAt(currentPositionInExpression);
                lastNumber.delete(0, lastNumber.length());
                ableToAppend = true;
            } else {
                if (currentLetterInExpression != ' ') {
                    if (ableToAppend) {
                        lastNumber.append(currentLetterInExpression);
                    } else {
                        printNotValidExpressionError();
                    }
                } else {
                    if (lastNumber.length() != 0) {
                        ableToAppend = false;
                    }
                }
            }
        }
        
        currentValueOfExpression = modifyValueWith(currentValueOfExpression, lastSign, lastNumber.toString());
        
        return currentValueOfExpression.toString(BASE);
    }
    
    private static String getRidOfMultiplicationAndDivision(String expression) {
        StringBuilder finalExpression = new StringBuilder(expression.length());
        StringBuilder lastSubExpression = new StringBuilder(expression.length());
        
        for (int currentPositionInExpression = 0;
                currentPositionInExpression < expression.length();
                currentPositionInExpression++) {
            char currentLetterInExpression = expression.charAt(currentPositionInExpression);
            if (findOperationType(currentLetterInExpression) == OperationType.ADDITION_OR_SUBTRACTION) {
                finalExpression.append(computeExpressionWithOperationType(lastSubExpression.toString(),
                                                                          OperationType.MULTIPLICATION_OR_DIVISION));
                finalExpression.append(currentLetterInExpression);
                lastSubExpression.delete(0, lastSubExpression.length());
            } else {
                lastSubExpression.append(currentLetterInExpression);
            }
        }
        finalExpression.append(computeExpressionWithOperationType(lastSubExpression.toString(),
                                                                  OperationType.MULTIPLICATION_OR_DIVISION));
        
        return finalExpression.toString();
    }
    
    private static String computeExpressionWithoutBrackets(String expression) {
        expression = getRidOfMultiplicationAndDivision(expression);
        expression = computeExpressionWithOperationType(expression, OperationType.ADDITION_OR_SUBTRACTION);
        return expression;
    }
    
    private static String computeExpression(String expression) {
        int currentBalance = 0;
        int firstOpenBracket = 0;
        StringBuilder finalExpression = new StringBuilder(expression.length());
        
        for (int currentPositionInExpression = 0;
                currentPositionInExpression < expression.length();
                currentPositionInExpression++) {
            switch (expression.charAt(currentPositionInExpression)) {
                case '(':
                    if (currentBalance == 0) {
                        firstOpenBracket = currentPositionInExpression;
                    }
                    currentBalance++;
                    break;
                case ')':
                    if (currentBalance == 0) {
                        printNotValidExpressionError();
                    }
                    currentBalance--;
                    if (currentBalance == 0) {
                        finalExpression.append(computeExpression(expression.substring(firstOpenBracket + 1,
                                                                                      currentPositionInExpression)));
                    }
                    break;
                default:
                    if (currentBalance == 0) {
                        finalExpression.append(expression.charAt(currentPositionInExpression));
                    }
                    break;
            }
        }
        if (currentBalance != 0) {
            printNotValidExpressionError();
        }
        
        return computeExpressionWithoutBrackets(finalExpression.toString());
    }
    
    private static boolean checkMinusForUnarity(String neighbourSymbols) {
        return neighbourSymbols.equals("==") || neighbourSymbols.equals("=+")
               || neighbourSymbols.equals("=-") || neighbourSymbols.equals("()")
               || neighbourSymbols.equals("(+") || neighbourSymbols.equals("(-");
    }
    
    private static String replaceBinaryMinuses(String expression) {
        StringBuilder finalExpression = new StringBuilder(expression.length());
        StringBuilder signsAndBrackets = new StringBuilder(expression.length());
        int currentPositionInSignsAndBrackets = -1;
        boolean addedSign = false;
        
        for (int currentPositionInExpression = 0;
                currentPositionInExpression < expression.length();
                currentPositionInExpression++) {
            char currentLetterInExpression = expression.charAt(currentPositionInExpression);
            if ((findOperationType(currentLetterInExpression) != OperationType.NOT_AN_OPERATION)
                    || (currentLetterInExpression == '-')) {
                signsAndBrackets.append(currentLetterInExpression);
                addedSign = true;
            } else if ((currentLetterInExpression == '(') || (currentLetterInExpression == ')')) {
                addedSign = false;
            } else if ((currentLetterInExpression != ' ') && !addedSign) {
                signsAndBrackets.append('+');
                addedSign = true;
            }
        }
        
        addedSign = false;
        
        for (int currentPositionInExpression = 0;
                currentPositionInExpression < expression.length();
                currentPositionInExpression++) {
            char currentLetterInExpression = expression.charAt(currentPositionInExpression);
            if ((findOperationType(currentLetterInExpression) != OperationType.NOT_AN_OPERATION)
                    || (currentLetterInExpression == '-')) {
                currentPositionInSignsAndBrackets++;
                addedSign = true;
            } else if ((currentLetterInExpression == '(') || (currentLetterInExpression == ')')) {
                addedSign = false;
            } else if ((currentLetterInExpression != ' ') && !addedSign) {
                currentPositionInSignsAndBrackets++;
                addedSign = true;
            }
            
            if (currentLetterInExpression == '-') {
                char leftNeighbour = (currentPositionInSignsAndBrackets == 0
                                      ? '='
                                      : signsAndBrackets.charAt(currentPositionInSignsAndBrackets - 1));
                char rightNeighbour = (currentPositionInSignsAndBrackets == signsAndBrackets.length() - 1
                                       ? '='
                                       : signsAndBrackets.charAt(currentPositionInSignsAndBrackets + 1));
                String neighbourSymbols = "";
                neighbourSymbols += leftNeighbour;
                neighbourSymbols += rightNeighbour;
                if (checkMinusForUnarity(neighbourSymbols)) {
                    finalExpression.append('-');
                } else {
                    finalExpression.append(BINARY_MINUS_REPLACEMENT);
                }
            } else {
                finalExpression.append(currentLetterInExpression);
            }
        }
        return finalExpression.toString();
    }
    
    private static String concatenateStrings(String[] stringsToConcatenate) {
        int wholeStringLength = 0;
        StringBuilder wholeString;
        
        for (String aStringToConcatenate : stringsToConcatenate) {
            wholeStringLength += aStringToConcatenate.length();
        }
        
        wholeString = new StringBuilder(wholeStringLength);
        
        for (String aStringToConcatenate : stringsToConcatenate) {
            wholeString.append(aStringToConcatenate);
            wholeString.append(" ");
        }
        
        return wholeString.toString();
    }
    
    public static void main(String[] args) {
        String expression = concatenateStrings(args);
        expression = replaceBinaryMinuses(expression);
        System.out.println(computeExpression(expression));
    }
}
