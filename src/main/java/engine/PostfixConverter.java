package engine;

import java.util.Stack;
import java.util.Vector;

public class PostfixConverter {
    
    private static boolean isOperator(String op) {
        return op.equals("and") || op.equals("or") || op.equals("xor");
    }

    private static int precedence(String op) {
        switch (op.toLowerCase()) {
            case "and":
                return 3;
            case "or":
                return 2;
            case "xor":
                return 1;
        }
        return -1;
    }

    public static Vector<Object> infixToPostfix(Vector<Object> infix) {
        Stack<String> stack = new Stack<>();
        Vector<Object> postfix = new Vector<>();

        for (Object token : infix) {
            if (token instanceof String) {
                String op = (String) token;
                while (!stack.isEmpty() && isOperator(stack.peek()) && precedence(op) <= precedence(stack.peek())) {
                    postfix.add(stack.pop());
                }
                stack.push(op);
            } else {
                postfix.add(token);
            }
        }

        while (!stack.isEmpty()) {
            postfix.add(stack.pop());
        }

        return postfix;
    }
}
