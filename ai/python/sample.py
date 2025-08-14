import torch
import torch.nn as nn
import torch.optim as optim

# 1. Tensors: The fundamental building blocks in PyTorch
# Tensors are similar to NumPy arrays, but can run on GPUs for accelerated computing.

# Create a 1D tensor (vector)
tensor_1d = torch.tensor([1, 2, 3, 4, 5])
print("1D Tensor (Vector):", tensor_1d)

# Create a 2D tensor (matrix)
tensor_2d = torch.tensor([[1, 2, 3], [4, 5, 6]])
print("2D Tensor (Matrix):\n", tensor_2d)

# Create a random tensor of a specific shape
random_tensor = torch.rand(2, 3) # 2 rows, 3 columns
print("Random Tensor (2x3):\n", random_tensor)

# Operations on tensors (similar to NumPy)
tensor_a = torch.tensor([[1.0, 2.0], [3.0, 4.0]])
tensor_b = torch.tensor([[5.0, 6.0], [7.0, 8.0]])

# Element-wise addition
sum_tensor = tensor_a + tensor_b
print("Element-wise addition:\n", sum_tensor)

# Matrix multiplication
matmul_tensor = torch.matmul(tensor_a, tensor_b)
print("Matrix multiplication:\n", matmul_tensor)

# Check if CUDA (GPU) is available and move a tensor to GPU if so
if torch.cuda.is_available():
    device = torch.device("cuda")
    print(f"\nUsing device: {device}")
    tensor_on_gpu = tensor_a.to(device)
    print("Tensor on GPU:\n", tensor_on_gpu)
else:
    device = torch.device("cpu")
    print(f"\nUsing device: {device}")


# 2. Building a Simple Neural Network (Feedforward Neural Network)

# Define a simple dataset (e.g., for XOR gate)
X_train = torch.tensor([[0.0, 0.0], [0.0, 1.0], [1.0, 0.0], [1.0, 1.0]], dtype=torch.float32)
y_train = torch.tensor([[0.0], [1.0], [1.0], [0.0]], dtype=torch.float32)

# Define the neural network model
class SimpleNN(nn.Module):
    def __init__(self):
        super(SimpleNN, self).__init__()
        # Input layer (2 features) to hidden layer (4 neurons)
        self.fc1 = nn.Linear(in_features=2, out_features=4)
        # Hidden layer (4 neurons) to output layer (1 neuron for binary output)
        self.fc2 = nn.Linear(in_features=4, out_features=1)

    def forward(self, x):
        # Apply ReLU activation function to the first layer's output
        x = torch.relu(self.fc1(x))
        # No activation function on the output layer for regression-like task (or Sigmoid for binary classification)
        x = self.fc2(x)
        return x

# Instantiate the model and move it to the appropriate device (CPU/GPU)
model = SimpleNN().to(device)
print("\nSimple Neural Network Model:\n", model)

# 3. Define Loss Function and Optimizer

# Loss function: Mean Squared Error (MSE) for regression/XOR
criterion = nn.MSELoss()

# Optimizer: Stochastic Gradient Descent (SGD)
# model.parameters() tells the optimizer which parameters to update
optimizer = optim.SGD(model.parameters(), lr=0.1) # Learning rate (lr)

# 4. Training Loop

num_epochs = 1000

print("\nStarting Training...")
for epoch in range(num_epochs):
    # Set the model to training mode
    model.train()

    # Forward pass: Compute predicted y by passing x to the model
    outputs = model(X_train.to(device))
    loss = criterion(outputs, y_train.to(device))

    # Backward pass and optimize
    optimizer.zero_grad() # Clear previous gradients
    loss.backward()       # Compute gradients of all variables with respect to loss
    optimizer.step()      # Perform updates using calculated gradients

    if (epoch + 1) % 100 == 0:
        print(f'Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}')

print("Training Complete!")

# 5. Evaluating the Model

# Set the model to evaluation mode
model.eval()

# Disable gradient calculation for inference
with torch.no_grad():
    test_data = torch.tensor([[0.0, 0.0], [0.0, 1.0], [1.0, 0.0], [1.0, 1.0]], dtype=torch.float32).to(device)
    predictions = model(test_data)
    print("\nPredictions for XOR input:")
    print(f"Input: [0.0, 0.0], Predicted: {predictions[0].item():.4f}")
    print(f"Input: [0.0, 1.0], Predicted: {predictions[1].item():.4f}")
    print(f"Input: [1.0, 0.0], Predicted: {predictions[2].item():.4f}")
    print(f"Input: [1.0, 1.0], Predicted: {predictions[3].item():.4f}")

# You might want to apply a threshold for binary classification
# For XOR, values close to 0 are 0, values close to 1 are 1.
binary_predictions = (predictions > 0.5).float()
print("\nBinary Predictions (threshold 0.5):")
print(f"Input: [0.0, 0.0], Predicted: {binary_predictions[0].item()}")
print(f"Input: [0.0, 1.0], Predicted: {binary_predictions[1].item()}")
print(f"Input: [1.0, 0.0], Predicted: {binary_predictions[2].item()}")
print(f"Input: [1.0, 1.0], Predicted: {binary_predictions[3].item()}")

# 6. Save and Load the Model (Optional)

# Save the entire model (architecture + learned parameters)
torch.save(model.state_dict(), 'simple_nn_model.pth')
print("\nModel saved to simple_nn_model.pth")

# To load the model:
# First, create an instance of the model architecture
loaded_model = SimpleNN().to(device)
# Then, load the saved state dictionary
loaded_model.load_state_dict(torch.load('simple_nn_model.pth'))
loaded_model.eval() # Set to evaluation mode after loading

with torch.no_grad():
    loaded_predictions = loaded_model(test_data)
    print("\nPredictions from loaded model:")
    print(f"Input: [0.0, 0.0], Predicted: {loaded_predictions[0].item():.4f}")